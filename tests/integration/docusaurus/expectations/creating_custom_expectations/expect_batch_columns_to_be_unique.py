"""
This is a template for creating custom BatchExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


# This class defines a Metric to support your Expectation.
# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py BatchColumnsUnique class_def">
class BatchColumnsUnique(TableMetricProvider):
    # </snippet>

    # This is the id string that will be used to reference your Metric.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py metric_name">
    metric_name = "table.columns.unique"
    # </snippet>

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py pandas">
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
        # </snippet>

    # @metric_value(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(
    #     cls,
    #     execution_engine,
    #     metric_domain_kwargs,
    #     metric_value_kwargs,
    #     metrics,
    #     runtime_configuration,
    # ):
    #     raise NotImplementedError
    #
    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine,
    #     metric_domain_kwargs,
    #     metric_value_kwargs,
    #     metrics,
    #     runtime_configuration,
    # ):
    #     raise NotImplementedError

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


# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py ExpectBatchColumnsToBeUnique class_def">
class ExpectBatchColumnsToBeUnique(BatchExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py docstring">
    """Expect batch to contain columns with unique contents."""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py examples">
    examples = [
        {
            "dataset_name": "expect_batch_columns_to_be_unique_1",
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
            "dataset_name": "expect_batch_columns_to_be_unique_2",
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
    # </snippet>
    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py metric_dependencies">
    metric_dependencies = ("table.columns.unique", "table.columns")
    # </snippet>

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("strict",)

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {"strict": True}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        strict = configuration.kwargs.get("strict")

        # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            assert (
                isinstance(strict, bool) or strict is None
            ), "strict must be a boolean value"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py validate">
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        unique_columns = metrics.get("table.columns.unique")
        batch_columns = metrics.get("table.columns")
        strict = configuration.kwargs.get("strict")

        duplicate_columns = unique_columns.symmetric_difference(batch_columns)

        if strict is True:
            success = len(duplicate_columns) == 0
        else:
            success = len(duplicate_columns) < len(batch_columns)

        return {
            "success": success,
            "result": {"observed_value": {"duplicate_columns": duplicate_columns}},
        }
        # </snippet>

    # This dictionary contains metadata for display in the public gallery
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py library_metadata">
    library_metadata = {
        "tags": ["uniqueness"],
        "contributors": ["@joegargery"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_batch_columns_to_be_unique.py diagnostics">
    ExpectBatchColumnsToBeUnique().print_diagnostic_checklist()
#     </snippet>

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectBatchColumnsToBeUnique().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
