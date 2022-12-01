import json
import logging
from typing import Callable, Dict, Optional

import numpy as np

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.exceptions.exceptions import InvalidExpectationKwargsError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_function_partial,
)
from great_expectations.expectations.metrics.import_manager import F, Window, sparktypes
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)


class ColumnValuesStringIntegersIncreasing(ColumnMapMetricProvider):
    function_metric_name = "column_values.string_integers.increasing"
    function_value_keys = ("column", "strictly")

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas(self, _column, **kwargs):
        if all(_column.str.isdigit()) is True:
            temp_column = _column.astype(int)
        else:
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        series_diff = np.diff(temp_column)

        strictly: Optional[bool] = kwargs.get("strictly") or False

        if strictly:
            return series_diff > 0
        else:
            return series_diff >= 0

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        column_name = metric_domain_kwargs["column"]
        table_columns = metrics["table.column_types"]
        column_metadata = [col for col in table_columns if col["name"] == column_name][
            0
        ]

        if isinstance(column_metadata["type"], (sparktypes.StringType)):
            column = F.col(column_name).cast(sparktypes.IntegerType())
        else:
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        compute_domain_kwargs = metric_domain_kwargs

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            compute_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )

        if any(np.array(df.select(column.isNull()).collect())):
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        diff = column - F.lag(column).over(Window.orderBy(F.lit("constant")))
        diff = F.when(diff.isNull(), 1).otherwise(diff)

        if metric_value_kwargs["strictly"] is True:
            diff = F.when(diff <= 0, F.lit(False)).otherwise(F.lit(True))
        else:
            diff = F.when(diff < 0, F.lit(False)).otherwise(F.lit(True))

        return (
            np.array(df.select(diff).collect()).reshape(-1)[1:],
            compute_domain_kwargs,
            accessor_domain_kwargs,
        )

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        table_domain_kwargs: dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
            metric_dependencies=None,
        )

        return dependencies


class ExpectColumnValuesToBeStringIntegersIncreasing(ColumnExpectation):
    """Expect a column to contain string-typed integers to be increasing.

    expect_column_values_to_be_string_integers_increasing is a :func:`column_map_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.


    Keyword Args:
        strictly (Boolean or None): \
            If True, values must be strictly greater than previous values

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
    See Also:
        :func:`expect_column_values_to_be_decreasing \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_be_decreasing>`
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation"],
        "contributors": ["@austiezr"],
    }

    map_metric = "column_values.string_integers.increasing"
    success_keys = ("strictly",)

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "strictly": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def _validate_success_key(
        param: str,
        required: bool,
        configuration: Optional[ExpectationConfiguration],
        validation_rules: Dict[Callable, str],
    ) -> None:
        """"""
        if param not in configuration.kwargs:
            if required:
                raise InvalidExpectationKwargsError(
                    f"Parameter {param} is required but was not found in configuration."
                )
            return

        param_value = configuration.kwargs[param]

        for rule, error_message in validation_rules.items():
            if not rule(param_value):
                raise InvalidExpectationKwargsError(error_message)

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration=configuration)

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:

        dependencies = super().get_validation_dependencies(
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        metric_kwargs = get_metric_kwargs(
            metric_name="column_values.string_integers.increasing.map",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )

        dependencies.set_metric_configuration(
            metric_name="column_values.string_integers.increasing.map",
            metric_configuration=MetricConfiguration(
                metric_name="column_values.string_integers.increasing.map",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Dict:

        string_integers_increasing = metrics.get(
            "column_values.string_integers.increasing.map"
        )

        success = all(string_integers_increasing[0])

        return ExpectationValidationResult(
            result={
                "observed_value": np.unique(
                    string_integers_increasing[0], return_counts=True
                )
            },
            success=success,
        )

    examples = [
        {
            "data": {
                "a": ["0", "1", "2", "3", "3", "9", "11"],
                "b": ["0", "1", "2", "3", "4", "9", "11"],
                "c": ["1", "2", "3", "3", "0", "6", "9"],
            },
            "tests": [
                {
                    "title": "positive_test_monotonic",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "strictly": False},
                    "out": {"success": True},
                },
                {
                    "title": "positive_test_strictly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "b", "strictly": True},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_monotonic",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "strictly": False},
                    "out": {"success": False},
                },
                {
                    "title": "negative_test_strictly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "strictly": True},
                    "out": {"success": False},
                },
            ],
        }
    ]


if __name__ == "__main__":
    ExpectColumnValuesToBeStringIntegersIncreasing().print_diagnostic_checklist()
