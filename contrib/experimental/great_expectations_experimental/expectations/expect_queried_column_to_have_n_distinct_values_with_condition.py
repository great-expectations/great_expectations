from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedColumnToHaveNDistinctValuesWithCondition(QueryExpectation):
    """Expect a column to have N distinct values, with an filter.

    Args:
        template_dict: dict with the following keys: \
            column_to_check (column to check uniqueness on. can be multiple column names separated by comma), \
            condition (the filter for boolean column, you can provide just the column name, evaluated to True), \
            num_of_distinct_values (number of distinct values the column is supposed ot have)
    """

    metric_dependencies = ("query.template_values",)

    query = """
            SELECT {column_to_check}
            FROM {active_batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            """

    success_keys = ("template_dict" "query",)

    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
        "row_condition",
        "condition_parser",
    )
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "query": query,
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        template_dict = self.validate_template_dict(configuration)
        query_result = metrics.get("query.template_values")
        actual_num_of_distinct_values = len(query_result)
        expected_num_of_distinct_values = template_dict["num_of_distinct_values"]

        if actual_num_of_distinct_values == expected_num_of_distinct_values:
            return {
                "result": {"observed_value": [list(row) for row in query_result]},
                "success": True,
            }
        else:
            return {
                "success": False,
                "result": {
                    "info": f"Expected {expected_num_of_distinct_values} but found {actual_num_of_distinct_values} distinct values",
                    "observed_value": query_result[:10],
                },
            }

    def validate_template_dict(self, configuration):
        template_dict = configuration.kwargs.get("template_dict")
        if not isinstance(template_dict, dict):
            raise TypeError("template_dict must be supplied as a dict")
        if not all(
            [
                "column_to_check" in template_dict,
                "condition" in template_dict,
                "num_of_distinct_values" in template_dict,
            ]
        ):
            raise KeyError(
                "The following keys have to be in the template dict: column_to_check, condition, num_of_distinct_values  "
            )
        return template_dict

    examples = [
        {
            "data": [
                {
                    "data": {
                        "uuid": [1, 2, 2, 3, 4, 4],
                        "is_open": [True, False, True, True, True, True],
                        "is_open_2": [False, True, False, False, False, True],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column_to_check": "uuid",
                            "condition": "is_open_2",
                            "num_of_distinct_values": 2,
                        }
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column_to_check": "uuid",
                            "condition": "is_open",
                            "num_of_distinct_values": 1,
                        }
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
            ],
        }
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@itaise"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnToHaveNDistinctValuesWithCondition().print_diagnostic_checklist()
