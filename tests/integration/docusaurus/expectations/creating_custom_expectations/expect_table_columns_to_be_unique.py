"""
This is a template for creating custom TableExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations
"""

from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics.metric_provider import (
    MetricConfiguration,
    MetricDomainTypes,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


# <snippet>
# This class defines a Metric to support your Expectation.
class TableColumnsUnique(TableMetricProvider):

    # This is the id string that will be used to reference your Metric.
    metric_name = "table.columns.unique"

    # This method implements the core logic for the PandasExecutionEngine
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        unique_columns = set(df.T.drop_duplicates().T.columns)

        return unique_columns

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        raise NotImplementedError

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        raise NotImplementedError

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


# </snippet>
# <snippet>
class ExpectTableColumnsToBeUnique(TableExpectation):
    """Expect table to contain columns with unique contents."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col1": [1, 2, 3, 4, 5],
                "col2": [2, 3, 4, 5, 6],
                "col3": [3, 4, 5, 6, 7],
            },
            "tests": [
                {
                    "title": "strict_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": True},
                    "out": {"success": True},
                }
            ],
        },
        {
            "data": {
                "col1": [1, 2, 3, 4, 5],
                "col2": [1, 2, 3, 4, 5],
                "col3": [3, 4, 5, 6, 7],
            },
            "tests": [
                {
                    "title": "loose_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": False},
                    "out": {"success": True},
                },
                {
                    "title": "strict_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": True},
                    "out": {"success": False},
                },
            ],
        },
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("table.columns.unique", "table.columns")

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("strict",)

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {"strict": True}

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
        if configuration is None:
            configuration = self.configuration

        strict = configuration.kwargs.get("strict")

        # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            assert (
                isinstance(strict, bool) or strict is None
            ), "strict must be a boolean value"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        return True

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        unique_columns = metrics.get("table.columns.unique")
        table_columns = metrics.get("table.columns")
        strict = configuration.kwargs.get("strict")

        duplicate_columns = unique_columns.symmetric_difference(table_columns)

        if strict is True:
            success = len(duplicate_columns) == 0
        else:
            success = len(duplicate_columns) < len(table_columns)

        return {
            "success": success,
            "result": {"observed_value": {"duplicate_columns": duplicate_columns}},
        }

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["uniqueness"],
        "contributors": ["@joegargery"],
    }


# </snippet>
if __name__ == "__main__":
    ExpectTableColumnsToBeUnique().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectTableColumnsToBeUnique().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_message"] is None
    assert check["stack_trace"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    assert check["passed"] is True
