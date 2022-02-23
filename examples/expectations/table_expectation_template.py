import json

# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
from typing import Any, Dict, Optional, Tuple

import numpy as np

from great_expectations.core import ExpectationConfiguration

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
    TableExpectation,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric import TableMetricProvider
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableAlphabeticalColumnNameCount(TableMetricProvider):

    # This is a built in metric - you do not have to implement it yourself. If you would like to use
    # a metric that does not yet exist, you can use the template below to implement it!
    metric_name = "table.alphabetical_column_name_count"

    # Below are metric computations for different dialects (Pandas, SqlAlchemy, Spark)
    # They can be used to compute the table data you will need to validate your Expectation
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        columns = metrics.get("table.columns")

        # For each column, testing if alphabetical and returning number of alphabetical columns
        return len([column for column in columns if column.isalpha()])

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        columns = metrics.get("table.columns")

        # For each column, testing if alphabetical and returning number of alphabetical columns
        return len([column for column in columns if column.isalpha()])

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        columns = metrics.get("table.columns")

        # For each column, testing if alphabetical and returning number of alphabetical columns
        return len([column for column in columns if column.isalpha()])

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectAlphabeticalColumnNameCountToEqual4(TableExpectation):
    """TODO: add a docstring here"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "columnone": [3, 5, 7],
                "columntwo": [True, False, True],
                "columnthree": ["a", "b", "c"],
                "columnfour": [None, 2, None],
            },
            "data_2": {
                "columnnumberone": [3, 5, 7],
                "columnnumbertwo": [True, False, True],
                "columnnumberthree": ["a", "b", "c"],
                "!_!!": ["c", "d", "e"],
            },
            "tests": [
                {
                    "title": "positive_test_with_4_alphabetical_columns",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"user_input": "helloWorld"},
                    "out": {
                        "success": True,
                        "observed_value": 4,
                    },
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
            #         "@your_name_here", # Don't forget to add your github handle here!
        ],
        "package": "experimental_expectations",
    }

    metric_dependencies = ("table.alphabetical_column_name_count",)
    success_keys = ("user_input",)

    default_kwarg_values = {
        "user_input": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
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

        #     # Setting up a configuration
        try:
            assert "user_input" in configuration.kwargs, "user_input is required"
            assert isinstance(
                configuration.kwargs["user_input"], str
            ), "user_input must be a string"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        super().validate_configuration(configuration)
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
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["value"])
        template_str = "Must have exactly 4 columns."
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

    # This method will utilize the computed metric to validate that your Expectation about the Table is true
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        actual_column_count = metrics.get("table.alphabetical_column_name_count")

        return {
            "success": actual_column_count == 4,
            "result": {"observed_value": actual_column_count},
        }


if __name__ == "__main__":
    ExpectAlphabeticalColumnNameCountToEqual4().print_diagnostic_checklist()
