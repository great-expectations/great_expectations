from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedColumnPairValuesToBeBothFilledOrNull(QueryExpectation):
    """Expect the values of a pair of columns to be either both filled or empty simultaneously.

     It checks if 2 columns are aligned - the values of each row need to either be both empty or filled.
     The expectation will fail if there's at least one row where one column is filled and the other isn't.

    Args:
    template_dict: dict with the following keys: \
        column_a (str): first column name, to compare values against column_b
        column_b (str): second column name, to compare values against column_a

    Returns:
            None
    """

    metric_dependencies = ("query.template_values",)

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

    success_keys = (
        "template_dict",
        "query",
    )

    domain_keys = ("batch_id", "row_condition", "condition_parser")

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

        template_dict = configuration.kwargs.get("template_dict")
        try:
            assert isinstance(
                template_dict, dict
            ), "'template_dict' must be supplied as a dict"
            assert all(
                [
                    "column_a" in template_dict,
                    "column_b" in template_dict,
                ]
            ), "The following keys must be in the template dict: column_a, column_b"
            assert isinstance(
                template_dict["column_a"], str
            ), "column_a must be a string"
            assert isinstance(
                template_dict["column_b"], str
            ), "column_b must be a string"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metrics = convert_to_json_serializable(data=metrics)
        try:
            num_of_inconsistent_rows = list(
                metrics.get("query.template_values")[0].values()
            )[0]
        except IndexError:
            raise IndexError("Invalid index - query.template_values has no [0] index]")

        is_success = not num_of_inconsistent_rows or num_of_inconsistent_rows == 0

        return {
            "success": is_success,
            "result": {
                "info": f"Row count with inconsistent values: {num_of_inconsistent_rows}"
            },
        }

    examples = [
        {
            "data": [
                {
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": [5, 0, 3, 1, 4],
                        "col3": ["a", "b", "c", "d", "e"],
                        "col4": [None, 6, 7, 9, 6],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test_same_type",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_a": "col1", "column_b": "col2"}},
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_negative_test_same_type",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_a": "col1", "column_b": "col4"}},
                    "out": {"success": False},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_positive_test_different_type",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_a": "col2", "column_b": "col3"}},
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_negative_test_different_type",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_a": "col3", "column_b": "col4"}},
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
    ExpectQueriedColumnPairValuesToBeBothFilledOrNull().print_diagnostic_checklist()
