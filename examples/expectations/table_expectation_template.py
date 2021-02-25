import json

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration, TableExpectation,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validator import Validator


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
from typing import Any, Dict, Optional, Tuple

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric import TableMetricProvider
from great_expectations.validator.validation_graph import MetricConfiguration


class TableColumnCountEquals4(TableMetricProvider):
    metric_name = "table.column_count_equals_4"

    # @metric_value(engine=PandasExecutionEngine)
    # def _pandas(
    #     cls,
    #     execution_engine: "ExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columns = metrics.get("table.columns")
    #     return len(columns) == 4
    #
    # @metric_value(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(
    #     cls,
    #     execution_engine: "ExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columns = metrics.get("table.columns")
    #     return len(columns) == 4
    #
    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine: "ExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columns = metrics.get("table.columns")
    #     return len(columns) == 4
    #
    # @classmethod
    # def _get_evaluation_dependencies(
    #     cls,
    #     metric: MetricConfiguration,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     execution_engine: Optional[ExecutionEngine] = None,
    #     runtime_configuration: Optional[dict] = None,
    # ):
    #     return {
    #         "table.column_count_equals_4": MetricConfiguration(
    #             "table.column_count_equals_4", metric.metric_domain_kwargs
    #         ),
    #     }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectTableColumnCountToEqual4(TableExpectation):
    """TODO: add a docstring here"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    # examples = [
    #     {
    #         "data": {
    #             "column_1": [3, 3, 3, 3, 3],
    #             "column_2": [2, 4, 5, 2, 3],
    #             "column_3": ["A", "B", "C", "D", "E"],
    #             "column_4": [True, False, True, True, False]
    #         },
    #         "tests": [
    #             {
    #                 "title": "positive_test_with_mostly",
    #                 "exact_match_out": False,
    #                 "include_in_gallery": True,
    #                 "in": {"df": "data"},
    #                 "out": {
    #                     "success": True,
    #                 },
    #             }
    #         ],
    #     }
    # ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            #         "@your_name_here", # Don't forget to add your github handle here!
        ],
        "package": "experimental_expectations",
    }

    metric_dependencies = ("table.column_count_in_range",)
    success_keys = ("range",)


    default_kwarg_values = {
        "range": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    """ A Metric Decorator for the Column Count"""

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

        # Setting up a configuration
        super().validate_configuration(configuration)

        # Ensuring that a proper value has been provided
        try:
            assert (
                    "value" in configuration.kwargs
            ), "An expected column count must be provided"
            assert isinstance(
                configuration.kwargs["value"], (int, dict)
            ), "Provided threshold must be an integer"
            if isinstance(configuration.kwargs["value"], dict):
                assert (
                        "$PARAMETER" in configuration.kwargs["value"]
                ), 'Evaluation Parameter dict for value kwarg must have "$PARAMETER" key.'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration=None,
            result=None,
            language=None,
            runtime_configuration=None,
            **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["value"])
        template_str = "Must have exactly $value columns."
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
        expected_column_count = configuration.kwargs.get("value")
        actual_column_count = metrics.get("table.column_count")

        return {
            "success": actual_column_count == expected_column_count,
            "result": {"observed_value": actual_column_count},
        }

if __name__ == "__main__":
    diagnostics = ExpectTableColumnCountToEqual4().run_diagnostics()
    print(json.dumps(diagnostics, indent=2))