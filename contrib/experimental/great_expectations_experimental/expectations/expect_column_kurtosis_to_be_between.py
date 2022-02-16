import json
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


class ColumnKurtosis(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.custom.kurtosis"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return stats.kurtosis(column)

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


class ExpectColumnKurtosisToBeBetween(ColumnExpectation):
    """Expect column Kurtosis to be between. Test values are drawn from various distributions (uniform, normal, gamma, student-t)"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [
                    -0.53292763,
                    -0.55002926,
                    -0.4438279,
                    0.86457185,
                    -0.36594115,
                    0.49479845,
                    -0.37942164,
                    0.56196101,
                    -0.87087446,
                    0.85122858,
                    0.50477797,
                    0.32140045,
                    0.67719681,
                    -0.79750033,
                    0.98015015,
                    0.71092908,
                    0.91318651,
                    0.84821723,
                    0.61791531,
                    0.27199073,
                ],  # drawn from uniform(-1, 1)
                "b": [
                    48.22849061,
                    47.6051427,
                    52.26539002,
                    48.72580962,
                    58.13226495,
                    50.00527448,
                    48.20014488,
                    40.18917266,
                    48.11166781,
                    54.24920618,
                    49.5333056,
                    47.88284549,
                    52.22758066,
                    45.52656677,
                    53.80837621,
                    47.64331079,
                    52.68390491,
                    57.65771134,
                    43.21413456,
                    61.01975259,
                ],  # drawn from normal(50, 5)
                "c": [
                    1.18456751,
                    1.62172915,
                    1.30161324,
                    1.23598756,
                    1.60049289,
                    1.07977883,
                    3.41474486,
                    1.72798224,
                    3.32432957,
                    2.18657412,
                    1.28335401,
                    1.71629046,
                    2.99812684,
                    1.65621709,
                    1.23124982,
                    1.32011043,
                    1.89228139,
                    1.55614664,
                    1.99860176,
                    1.06292023,
                ],  # drawn from gamma(1,1)
                "d": [
                    -1.13752893e00,
                    2.92873190e00,
                    -1.72513718e00,
                    5.69411070e-02,
                    -4.18521597e-01,
                    -1.59507748e00,
                    -4.13968768e00,
                    1.84869478e-02,
                    3.58256016e00,
                    -9.51684014e-02,
                    1.82473847e-01,
                    2.54814625e-01,
                    -1.40049841e-01,
                    -2.13430996e00,
                    2.17116735e01,
                    5.72282900e-01,
                    -4.68429437e-01,
                    7.16024818e-01,
                    -2.33349034e00,
                    -6.24536666e00,
                ],  # drawn from student-t(2)
            },
            "tests": [
                {
                    "title": "positive_test_neg_kurtosis",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "min_value": -1000, "max_value": 0},
                    "out": {"success": True, "observed_value": -1.3313434082203324},
                },
                {
                    "title": "negative_test_no_kurtosis",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "b", "min_value": 1, "max_value": None},
                    "out": {"success": False, "observed_value": -0.09728207073685091},
                },
                {
                    "title": "positive_test_small_kurtosis",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "min_value": -1, "max_value": 1},
                    "out": {"success": True, "observed_value": 0.6023057552879445},
                },
                {
                    "title": "negative_test_large_kurtosis",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "d", "min_value": -1, "max_value": 1},
                    "out": {"success": False, "observed_value": 10.0205745719473},
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@lodeous",
            "@bragleg",
            "@rexboyce",
        ],
        "package": "experimental_expectations",
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.kurtosis",)
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
    #     necessary configuration arguments have been provided for the validation of the expectation.
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.custom.kurtosis",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    self_check_report = ExpectColumnKurtosisToBeBetween().run_diagnostics()
    print(json.dumps(self_check_report, indent=2))
