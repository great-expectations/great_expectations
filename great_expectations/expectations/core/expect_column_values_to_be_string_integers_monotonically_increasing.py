import inspect
import logging
from typing import Dict, Optional, Callable

import numpy as np
import pandas as pd

from great_expectations.expectations.metrics.column_map_metrics.column_values_string_integers_monotonically_increasing \
    import ColumnValuesStringIntegersMonotonicallyIncreasing
# from great_expectations.expectations.metrics.column_map_metrics.column_values_increasing import ColumnValuesIncreasing
# from great_expectations.expectations.metrics.column_map_metrics.column_values_of_type import ColumnValuesOfType
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import InvalidExpectationConfigurationError, InvalidExpectationKwargsError
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sparkdf_execution_engine import SparkDFExecutionEngine

from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing
)

from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    TableExpectation,
    ColumnExpectation
)

from great_expectations.validator.metric_configuration import MetricConfiguration

from great_expectations.expectations.util import render_evaluation_parameter_string

logger = logging.getLogger(__name__)

try:
    import pyspark.sql.types as sparktypes
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )

try:
    import sqlalchemy as sa
    from sqlalchemy.dialects import registry
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support."
    )
    sa = None
    registry = None

try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None

try:
    import pybigquery.sqlalchemy_bigquery

    registry.register("bigquery", "pybigquery.sqlalchemy_bigquery", "dialect")

    try:
        getattr(pybigquery.sqlalchemy_bigquery, "INTEGER")
        bigquery_types_tuple = None
    except AttributeError:
        logger.warning(
            "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
        )
        from collections import namedtuple

        BigQueryTypes = namedtuple(
            "BigQueryTypes", sorted(pybigquery.sqlalchemy_bigquery._type_map)
        )
        bigquery_types_tuple = BigQueryTypes(**pybigquery.sqlalchemy_bigquery._type_map)
except ImportError:
    bigquery_types_tuple = None
    pybigquery = None

class ExpectColumnValuesToBeStringIntegersMonotonicallyIncreasing(ColumnExpectation):
    """Expect a column to contain string-typed integers to be monotonically increasing.

    expect_column_values_to_be_string_integers_monotonically_increasing is a :func:`column_map_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

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
        "maturity": "production",
        "package": "great_expectations",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    map_metric = "column_values.string_integers.monotonically_increasing"

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
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
            self,
            configuration: Optional[ExpectationConfiguration]
    ) -> bool:
        return super().validate_configuration(configuration=configuration)


    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> dict:
        # metric_config = MetricConfiguration(
        #     metric_name="column_values.string_integers.monotonically_increasing.map",
        #     metric_domain_kwargs=ColumnValuesStringIntegersMonotonicallyIncreasing.default_kwarg_values
        # )

        dependencies = super().get_validation_dependencies(
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration
        )
        # dependencies["metrics"]["column_values.string_integers.monotonically_increasing"] = metric_config

        column_value_increasing_metric_kwargs = get_metric_kwargs(
            metric_name="column_values.string_integers.monotonically_increasing.map",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        dependencies["metrics"]["column_values.string_integers.monotonically_increasing"] = MetricConfiguration(
            metric_name="column_values.string_integers.monotonically_increasing.map",
            metric_domain_kwargs=column_value_increasing_metric_kwargs["metric_domain_kwargs"],
            metric_value_kwargs=column_value_increasing_metric_kwargs["metric_domain_kwargs"]
        )
        #
        # column_value_type_metric_kwargs = get_metric_kwargs(
        #     metric_name="column_values.of_type",
        #     configuration=configuration,
        #     runtime_configuration=runtime_configuration,
        # )
        # dependencies["metrics"]["column_values.of_type"] = MetricConfiguration(
        #     metric_name="column_values.of_type",
        #     metric_domain_kwargs=column_value_type_metric_kwargs["metric_domain_kwargs"],
        #     metric_value_kwargs=column_value_type_metric_kwargs["metric_value_kwargs"]
        # )

        return dependencies


    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Dict:
        print(metrics.keys())

        # column_values_of_type = execution_engine.resolve_metrics(
        #     metrics["column_values.of_type"],
        # )
        # column_values_increasing = metrics.get("column_values.increasing")
        #
        # if isinstance(execution_engine, PandasExecutionEngine):
        #     return self._validate_pandas(
        #         column_values_of_type=column_values_of_type,
        #         column_values_increasing=column_values_increasing,
        #         expected_type=str
        #     )

        return False


    @staticmethod
    def _validate_pandas(
            self,
            column_values_of_type,
            column_values_increasing,
            expected_type
    ):
        comp_types = [np.dtype(expected_type).type,
                      pd.StringDtype,
                      pd.core.dtypes.dtypes.str_type]

        native_type = _native_type_type_map(expected_type)
        if native_type is not None:
            comp_types.extend(native_type)

        type_success = True if all(column_values_of_type.type) else False

        increasing_success = True if column_values_increasing else False

        success = True if type_success is True & increasing_success is True else False
        return {
            "success": success,
            "result": {
                "observed_value": column_values_of_type.value_counts(
                    normalize=True
                )
            }
        }
