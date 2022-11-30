import json
import logging
import traceback
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.stats as stats

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
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)

try:
    from sqlalchemy.exc import ProgrammingError
    from sqlalchemy.sql import Select
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    ProgrammingError = None
    Select = None

try:
    from sqlalchemy.engine.row import Row
except ImportError:
    try:
        from sqlalchemy.engine.row import RowProxy

        Row = RowProxy
    except ImportError:
        logger.debug(
            "Unable to load SqlAlchemy Row class; please upgrade you sqlalchemy installation to the latest version."
        )
        RowProxy = None
        Row = None


class ColumnSkew(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.custom.skew"
    value_keys = ("abs",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, abs=False, **kwargs):
        if abs:
            return np.abs(stats.skew(column))
        return stats.skew(column)

    @metric_value(engine=SqlAlchemyExecutionEngine)
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

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        sqlalchemy_engine = execution_engine.engine
        dialect = sqlalchemy_engine.dialect

        column_mean = _get_query_result(
            func=sa.func.avg(column * 1.0),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_count = _get_query_result(
            func=sa.func.count(column),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        if dialect.name.lower() == "mssql":
            standard_deviation = sa.func.stdev(column)
        else:
            standard_deviation = sa.func.stddev_samp(column)

        column_std = _get_query_result(
            func=standard_deviation,
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_third_moment = _get_query_result(
            func=sa.func.sum(sa.func.pow(column - column_mean, 3)),
            selectable=selectable,
            sqlalchemy_engine=sqlalchemy_engine,
        )

        column_skew = column_third_moment / (column_std**3) / (column_count - 1)
        if metric_value_kwargs["abs"]:
            return np.abs(column_skew)
        else:
            return column_skew


def _get_query_result(func, selectable, sqlalchemy_engine):
    simple_query: Select = sa.select(func).select_from(selectable)

    try:
        result: Row = sqlalchemy_engine.execute(simple_query).fetchone()[0]
        return result
    except ProgrammingError as pe:
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe()

        #
    # @metric_value(engine=SparkDFExecutionEngine, metric_fn_type="value")
    # def _spark(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         df,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column = accessor_domain_kwargs["column"]
    #
    #     column_median = None
    #
    #     # TODO: compute the value and return it
    #
    #     return column_median
    #
    # @classmethod
    # def _get_evaluation_dependencies(
    #     cls,
    #     metric: MetricConfiguration,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     execution_engine: Optional[ExecutionEngine] = None,
    #     runtime_configuration: Optional[dict] = None,
    # ):
    #     """This should return a dictionary:
    #
    #     {
    #       "dependency_name": MetricConfiguration,
    #       ...
    #     }
    #     """
    #
    #     dependencies = super()._get_evaluation_dependencies(
    #         metric=metric,
    #         configuration=configuration,
    #         execution_engine=execution_engine,
    #         runtime_configuration=runtime_configuration,
    #     )
    #
    #     table_domain_kwargs = {
    #         k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
    #     }
    #
    #     dependencies.update(
    #         {
    #             "table.row_count": MetricConfiguration(
    #                 "table.row_count", table_domain_kwargs
    #             )
    #         }
    #     )
    #
    #     if isinstance(execution_engine, SqlAlchemyExecutionEngine):
    #         dependencies["column_values.nonnull.count"] = MetricConfiguration(
    #             "column_values.nonnull.count", metric.metric_domain_kwargs
    #         )
    #
    #     return dependencies


class ExpectColumnSkewToBeBetween(ColumnExpectation):
    """Expect column skew to be between. Currently tests against Gamma and Beta distributions"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [
                    5.27071512,
                    7.05981507,
                    8.46671693,
                    10.20629973,
                    6.15519149,
                    7.11709362,
                    5.31915535,
                    6.56441299,
                    5.69143401,
                    5.0389317,
                    6.48222587,
                    5.62433534,
                    5.46219467,
                    5.74686441,
                    6.05413964,
                    7.09435276,
                    6.43876861,
                    6.05301145,
                    6.12727457,
                    6.80603351,
                ],  # sampled from Gamma(1, 5)
                "b": [
                    81.11265955,
                    76.7836479,
                    85.25019592,
                    93.93285666,
                    83.63587009,
                    81.88712944,
                    80.37321975,
                    86.786491,
                    80.05277435,
                    70.36302516,
                    79.4907302,
                    84.1288281,
                    87.79298488,
                    78.02771047,
                    80.63975023,
                    88.59461893,
                    84.05632481,
                    84.54128192,
                    78.74152549,
                    83.60684806,
                ],  # sampled from Beta(50, 10)
                "c": [
                    95.74648827,
                    80.4031074,
                    85.41863916,
                    93.98001949,
                    97.84607818,
                    89.01205412,
                    89.55045229,
                    97.32734707,
                    93.94199505,
                    88.19992377,
                    98.3336087,
                    97.66984436,
                    97.39464709,
                    95.55637873,
                    96.10980996,
                    90.18004343,
                    96.2019293,
                    89.19519753,
                    94.01807868,
                    93.23978285,
                ],  # sampled from Beta(20, 2)
            },
            "tests": [
                {
                    "title": "positive_test_positive_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "a", "min_value": 0.25, "max_value": 10},
                    "out": {"success": True, "observed_value": 1.6974323016687487},
                },
                {
                    "title": "negative_test_no_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "b", "min_value": 0.25, "max_value": 10},
                    "out": {"success": False, "observed_value": -0.07638895580386174},
                },
                {
                    "title": "positive_test_negative_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {"column": "c", "min_value": -10, "max_value": -0.5},
                    "out": {"success": True, "observed_value": -0.9979514313860596},
                },
                {
                    "title": "negative_test_abs_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {
                        "column": "c",
                        "abs": True,
                        "min_value": 0,
                        "max_value": 0.5,
                    },
                    "out": {"success": False, "observed_value": 0.9979514313860596},
                },
                {
                    "title": "positive_test_abs_skew",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "tolerance": 0.1,
                    "in": {
                        "column": "c",
                        "abs": True,
                        "min_value": 0.5,
                        "max_value": 10,
                    },
                    "out": {"success": True, "observed_value": 0.9979514313860596},
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                {
                    "backend": "sqlalchemy",
                    "dialects": ["mysql", "postgresql"],
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@lodeous",
            "@rexboyce",
            "@bragleg",
        ],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.skew",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max", "abs")

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "abs": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    # def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
    #     """
    #     Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
    #     necessary configuration arguments have been provided for the validation of the expectation.
    #
    #     Args:
    #         configuration (OPTIONAL[ExpectationConfiguration]): \
    #             An optional Expectation Configuration entry that will be used to configure the expectation
    #     Returns:
    #         None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
    #     """
    #     super().validate_configuration(configuration)
    #     self.validate_metric_value_between_configuration(configuration=configuration)

    # @classmethod
    # @renderer(renderer_type="renderer.prescriptive")
    # @render_evaluation_parameter_string
    # def _prescriptive_renderer(
    #     cls,
    #     configuration=None,
    #     result=None,
    #     runtime_configuration=None,
    #     **kwargs,
    # ):
    #     runtime_configuration = runtime_configuration or {}
    #     include_column_name = False if runtime_configuration.get("include_column_name") is False else True
    #     styling = runtime_configuration.get("styling")
    #     params = substitute_none_for_missing(
    #         configuration.kwargs,
    #         [
    #             "column",
    #             "min_value",
    #             "max_value",
    #             "row_condition",
    #             "condition_parser",
    #             "strict_min",
    #             "strict_max",
    #         ],
    #     )
    #
    #     if (params["min_value"] is None) and (params["max_value"] is None):
    #         template_str = "median may have any numerical value."
    #     else:
    #         at_least_str, at_most_str = handle_strict_min_max(params)
    #         if params["min_value"] is not None and params["max_value"] is not None:
    #             template_str = f"median must be {at_least_str} $min_value and {at_most_str} $max_value."
    #         elif params["min_value"] is None:
    #             template_str = f"median must be {at_most_str} $max_value."
    #         elif params["max_value"] is None:
    #             template_str = f"median must be {at_least_str} $min_value."
    #
    #     if include_column_name:
    #         template_str = "$column " + template_str
    #
    #     if params["row_condition"] is not None:
    #         (
    #             conditional_template_str,
    #             conditional_params,
    #         ) = parse_row_condition_string_pandas_engine(params["row_condition"])
    #         template_str = conditional_template_str + ", then " + template_str
    #         params.update(conditional_params)
    #
    #     return [
    #         RenderedStringTemplateContent(
    #             **{
    #                 "content_block_type": "string_template",
    #                 "string_template": {
    #                     "template": template_str,
    #                     "params": params,
    #                     "styling": styling,
    #                 },
    #             }
    #         )
    #     ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.custom.skew",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    ExpectColumnSkewToBeBetween().print_diagnostic_checklist()
