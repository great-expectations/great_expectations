"""
This is a template for creating custom QueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


# This class defines the Expectation itself
class ExpectQueriedColumnPairValuesToBeConsistent(QueryExpectation):
    """Expect the values of a pair of columns to be either both filled or empty for each row"""


    # This is the id string of the Metric(s) used by this Expectation.
    metric_dependencies = ("query.template_values",)

    # This is the default, baked-in SQL Query for this QueryExpectation
    # query = """
    #         SQL QUERY GOES HERE
    #         """
    query = """
        SELECT
            COUNT(1)
        FROM
            {active_batch}
        WHERE
            ({column_a} is not null and {column_b} is null)
            OR
            ({column_a} is null and {column_b} is not null)
            """

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("template_dict", "query",)

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "query": query,
    }

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
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        # raise NotImplementedError

        metrics = convert_to_json_serializable(data=metrics)
        num_of_non_consistent_rows = list(metrics.get("query.template_values")[0].values())[0]

        if not num_of_non_consistent_rows or num_of_non_consistent_rows == 0:
            return {
                "info": "",
                "success": True,
            }

        else:
            return {
                "success": False,
                "result": {
                    "info": f"{num_of_non_consistent_rows} inconsistent rows",
                    "observed_value": num_of_non_consistent_rows,
                },
            }

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.

    examples = [
        {
            "data": [
                {
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": [2, 3, 4, 5, 6],
                        "col3": [None, 6, 7, 9, 6],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {"column_a": "col1", "column_b": "col2"}
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {"column_a": "col1", "column_b": "col3"}
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite"],
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@eden-o"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnPairValuesToBeConsistent().print_diagnostic_checklist()
