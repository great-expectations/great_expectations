"""
This is a template for creating custom QueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Any, Dict, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py ExpectQueryToMatchSomeCriteria class_def">
class ExpectQueryToMatchSomeCriteria(QueryExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py docstring">
    """TODO: Add a docstring here"""
    # </snippet>

    # This is the id string of the Metric(s) used by this Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py metric_dependencies">
    metric_dependencies = ("METRIC NAME GOES HERE",)
    # </snippet>

    # This is the default, baked-in SQL Query for this QueryExpectation
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py sql_query">
    query = """
            SQL QUERY GOES HERE
            """
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py success_keys">
    success_keys = ("query",)
    # </snippet>

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "query": query,  # Passing the above `query` attribute here as a default kwarg allows for the Expectation to be run with the defaul query, or have that query overridden by passing a `query` kwarg into the expectation
    }

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
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py _validate">
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        raise NotImplementedError

    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py examples">
    examples = []
    # </snippet>

    # This dictionary contains metadata for display in the public gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py print_diagnostic_checklist">
    ExpectQueryToMatchSomeCriteria().print_diagnostic_checklist()
    # </snippet>
