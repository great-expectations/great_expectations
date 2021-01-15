import json

from typing import Any, Dict, Optional, Tuple


import numpy as np
import pandas as pd

from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
    _format_map_output,
)


from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
    num_to_str,
    substitute_none_for_missing,
)

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
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.validator.validation_graph import MetricConfiguration


examples = [
    {
        "data": {"a": [1, 2, 3, 4], "b": [1, 2, 2, 3], "c": [5, 7, 6, None]},
        "schemas": {
            "spark": {"a": "IntegerType", "b": "IntegerType", "c": "IntegerType"}
        },
        "tests": [
            {
                "title": "positive_test_min_equal_max",
                "exact_match_out": False,
                "in": {"column": "a", "min_value": 2.5, "max_value": 2.5},
                "out": {"success": True, "observed_value": 2.5},
            },
            {
                "title": "positive_test_null_min",
                "exact_match_out": False,
                "in": {"column": "a", "min_value": None, "max_value": 3},
                "out": {"success": True, "observed_value": 2.5},
            },
            {
                "title": "negative_test_missing_value_in_column_complete_result_format",
                "exact_match_out": True,
                "in": {
                    "column": "c",
                    "min_value": 7,
                    "max_value": 7,
                    "result_format": "COMPLETE",
                },
                "out": {
                    "success": False,
                    "result": {
                        "observed_value": 6.0,
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
                "title": "test_empty_column_should_be_false_no_observed_value_with_which_to_compare",
                "exact_match_out": False,
                "in": {
                    "column": "empty_column",
                    "min_value": 0,
                    "max_value": 0,
                    "catch_exceptions": False,
                },
                "out": {"success": False, "observed_value": None},
            }
        ],
    },
]


class ColumnCustomMedian(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.custom.median"

    # @column_aggregate_value(engine=PandasExecutionEngine)
    # def _pandas(cls, column, **kwargs):
    #     column_median = None
    #
    #     # TODO: compute the value and return it
    #
    #     return column_median
    #
    # @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
    # def _sqlalchemy(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         selectable,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column_name = accessor_domain_kwargs["column"]
    #     column = sa.column(column_name)
    #     sqlalchemy_engine = execution_engine.engine
    #     dialect = sqlalchemy_engine.dialect
    #
    #     column_median = None
    #
    #     # TODO: compute the value and return it
    #
    #     return column_median
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


class ExpectColumnCustomMedianToBeBetween(ColumnExpectation):
    """Expect the column median to be between a minimum value and a maximum value.

            expect_column_median_to_be_between is a \
            :func:`column_aggregate_expectation \
            <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

            Args:
                column (str): \
                    The column name.
                min_value (int or None): \
                    The minimum value for the column median.
                max_value (int or None): \
                    The maximum value for the column median.
                strict_min (boolean):
                    If True, the column median must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the column median must be strictly smaller than max_value, default=False

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
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.

            Returns:
                An ExpectationSuiteValidationResult

                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

            Notes:
                These fields in the result object are customized for this expectation:
                ::

                    {
                        "observed_value": (float) The true median for the column
                    }

                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound
                * If max_value is None, then min_value is treated as a lower bound

            See Also:
                :func:`expect_column_mean_to_be_between \
                <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_mean_to_be_between>`

                :func:`expect_column_stdev_to_be_between \
                <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_stdev_to_be_between>`

            """

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.median",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    # def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
    #     """
    #     Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
    #     neccessary configuration arguments have been provided for the validation of the expectation.
    #
    #     Args:
    #         configuration (OPTIONAL[ExpectationConfiguration]): \
    #             An optional Expectation Configuration entry that will be used to configure the expectation
    #     Returns:
    #         True if the configuration has been validated successfully. Otherwise, raises an exception
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
    #     language=None,
    #     runtime_configuration=None,
    #     **kwargs,
    # ):
    #     runtime_configuration = runtime_configuration or {}
    #     include_column_name = runtime_configuration.get("include_column_name", True)
    #     include_column_name = (
    #         include_column_name if include_column_name is not None else True
    #     )
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

    # def _validate(
    #     self,
    #     configuration: ExpectationConfiguration,
    #     metrics: Dict,
    #     runtime_configuration: dict = None,
    #     execution_engine: ExecutionEngine = None,
    # ):
    #     return self._validate_metric_value_between(
    #         metric_name="column.custom.median",
    #         configuration=configuration,
    #         metrics=metrics,
    #         runtime_configuration=runtime_configuration,
    #         execution_engine=execution_engine,
    #     )


if __name__ == "__main__":
    self_check_report = ExpectColumnCustomMedianToBeBetween().self_check()
    print(json.dumps(self_check_report, indent=2))
