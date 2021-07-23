import json
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats as stats

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


class ColumnNormallyDistributed(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.custom.normally_distributed"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return stats.normaltest(column, nan_policy="omit")[1]  # Should we omit nan?

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


class ExpectColumnValuesToBeNormallyDistributed(ColumnExpectation):
    """Test whether column values are normally distributed. NaN values are omitted."""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [  # this was drawn from the normal distribution
                    -0.42559356,
                    1.71053911,
                    -0.33074949,
                    -0.51614177,
                    -0.61934564,
                    1.1351354,
                    1.39973079,
                    -0.02995425,
                    0.84342204,
                    2.11280806,
                ],
                "b": [  # this was drawn from the gamma distribution
                    1.43829829,
                    5.73385056,
                    1.77222341,
                    0.50729875,
                    0.34536101,
                    1.54515905,
                    1.11811223,
                    0.8430591,
                    0.80270869,
                    1.02455144,
                ],
            },
            "tests": [
                {
                    "title": "passes",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "a", "min_value": 0.01},
                    "out": {"success": True, "observed_value": 0.20745969675492978},
                },
                {
                    "title": "fails",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "b", "min_value": 0.01},
                    "out": {"success": False, "observed_value": 1.369758134334228e-05},
                },
            ],
        },
    ]

    library_metadata = {
        "maturity": "experimental",
        "package": "great_expectations_experimental",
        "tags": ["experimental"],
        "contributors": [
            "@ljohnston931",
            "@rexboyce",
            "@lodeous",
            "@sophiarawlings",
            "@vtdangg",
        ],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.normally_distributed",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
    )

    # Default values
    default_kwarg_values = {
        "min_value": 0.01,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

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
            metric_name="column.custom.normally_distributed",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )


if __name__ == "__main__":
    self_check_report = ExpectColumnValuesToBeNormallyDistributed().run_diagnostics()
    print(json.dumps(self_check_report, indent=2))
