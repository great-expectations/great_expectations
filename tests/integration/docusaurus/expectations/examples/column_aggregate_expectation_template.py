"""
This is a template for creating custom ColumnAggregateExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnAggregateExpectations, the main business logic for calculation will live in this class.
# <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py ColumnAggregateMatchesSomeCriteria class_def">
class ColumnAggregateMatchesSomeCriteria(ColumnAggregateMetricProvider):
    # </snippet>

    # This is the id string that will be used to reference your Metric.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py metric_name">
    metric_name = "METRIC NAME GOES HERE"
    # </snippet>

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py pandas">
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        raise NotImplementedError

    # This method defines the business logic for evaluating your Metric when using a SqlAlchemyExecutionEngine
    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError
    #
    # This method defines the business logic for evaluating your Metric when using a SparkDFExecutionEngine
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py ExpectColumnAggregateToMatchSomeCriteria class_def">
class ExpectColumnAggregateToMatchSomeCriteria(ColumnAggregateExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py docstring">
    """TODO: add a docstring here"""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py examples">
    examples = []
    # </snippet>

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py metric_dependencies">
    metric_dependencies = ("METRIC NAME GOES HERE",)
    # </snippet>

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
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

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py validate">
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        # </snippet>
        raise NotImplementedError

    # This object contains metadata for display in the public Gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


#     </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_aggregate_expectation_template.py diagnostics">
    ExpectColumnAggregateToMatchSomeCriteria().print_diagnostic_checklist()
#     </snippet>
