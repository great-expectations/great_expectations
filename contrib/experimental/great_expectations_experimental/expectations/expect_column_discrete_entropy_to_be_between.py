import json
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.stats

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnDiscreteEntropy(ColumnMetricProvider):
    """MetricProvider Class for Discrete Entropy MetricProvider"""

    metric_name = "column.discrete.entropy"
    value_keys = ("base",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, base, **kwargs):
        column_value_counts = column.value_counts()
        return scipy.stats.entropy(column_value_counts, base=base)

    @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        base = metric_value_kwargs["base"]

        column_value_counts = metrics.get("column.value_counts")
        return scipy.stats.entropy(column_value_counts, base=base)

    @metric_value(engine=SparkDFExecutionEngine, metric_fn_type="value")
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        base = metric_value_kwargs["base"]

        column_value_counts = metrics.get("column.value_counts")
        return scipy.stats.entropy(column_value_counts, base=base)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):

        dependencies = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }

        dependencies.update(
            {
                "table.row_count": MetricConfiguration(
                    "table.row_count", table_domain_kwargs
                )
            }
        )

        if isinstance(execution_engine, SqlAlchemyExecutionEngine) or isinstance(
            execution_engine, SparkDFExecutionEngine
        ):
            dependencies["column_values.nonnull.count"] = MetricConfiguration(
                "column_values.nonnull.count", metric.metric_domain_kwargs
            )
            dependencies["column.value_counts"] = MetricConfiguration(
                metric_name="column.value_counts",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )
        return dependencies


class ExpectColumnDiscreteEntropyToBeBetween(ColumnExpectation):
    """Expect the column discrete entropy to be between a minimum value and a maximum value.
            The Shannon entropy of a discrete probability distribution is given by
            - \\sum_{i=1}^{n} P(x_i) * \\log(P(x_i))
            where P(x_i) is the probability of occurrence of value x_i.
            For observed data the P(x_i) are replaced by the empirical frequencies n_{x_i}/N

            expect_column_discrete_entropy_to_be_between is a \
            :func:`column_aggregate_expectation
            <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.
            Args:
                column (str): \
                    The column name.
                min_value (float or None): \
                    The minimum value for the column standard deviation.
                max_value (float or None): \
                    The maximum value for the column standard deviation.
                strict_min (boolean):
                    If True, the column standard deviation must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the column standard deviation must be strictly smaller than max_value, default=False
                base (float):
                    The base of the logarithm, default=2
            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                catch_exceptions (boolean or None): \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.
            Returns:
                An ExpectationSuiteValidationResult
                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
            Notes:
                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound
                * If max_value is None, then min_value is treated as a lower bound
            """

    examples = [
        {
            "data": {
                "a": [1, 2, 3, 4],
                "b": ["Jarndyce", "Jarndyce", None, None],
                "c": ["past", "present", "future", None],
            },
            "schemas": {
                "spark": {"a": "IntegerType", "b": "StringType", "c": "StringType"}
            },
            "tests": [
                {
                    "title": "positive_test_min_equal_max",
                    "exact_match_out": False,
                    "in": {"column": "a", "min_value": 2.0, "max_value": 2.0},
                    "out": {"success": True, "observed_value": 2.0},
                },
                {
                    "title": "positive_test_null_min",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "b", "min_value": None, "max_value": 1},
                    "out": {"success": True, "observed_value": 0.0},
                },
                {
                    "title": "negative_test_nondefault_kwargs_complete_result_format",
                    "include_in_gallery": True,
                    "exact_match_out": True,
                    "in": {
                        "column": "c",
                        "min_value": 7,
                        "max_value": 7,
                        "base": 3,
                        "result_format": "COMPLETE",
                    },
                    "out": {
                        "success": False,
                        "result": {
                            "observed_value": 1.0,
                            "element_count": 4,
                            "missing_count": 1,
                            "missing_percent": 25.0,
                        },
                    },
                },
            ],
        },
        {
            "data": {"empty_column": []},
            "schemas": {"spark": {"empty_column": "IntegerType"}},
            "tests": [
                {
                    "title": "test_empty_column_should_be_zero",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {
                        "column": "empty_column",
                        "min_value": 0,
                        "max_value": 0,
                        "catch_exceptions": False,
                    },
                    "out": {"success": True, "observed_value": 0.0},
                }
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["experimental"],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@edjoesu",
        ],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.discrete.entropy",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
        "base",
    )

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "base": 2,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        neccessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "discrete entropy may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"discrete entropy must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"discrete entropy must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"discrete entropy must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.discrete.entropy",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    ExpectColumnDiscreteEntropyToBeBetween().print_diagnostic_checklist()
