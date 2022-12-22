"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration

# TODO: <Alex>ALEX</Alex>
from great_expectations.core.util import convert_to_json_serializable

# TODO: <Alex>ALEX</Alex>
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedTableRowCountToBe(QueryExpectation):
    """Expect the expect the number of rows returned from a queried table to equal a specified value."""

    metric_dependencies = ("query.table",)

    query = """
            SELECT COUNT(*)
            FROM {active_batch}
            """

    success_keys = (
        "value",
        "query",
    )

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "value": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        value = configuration["kwargs"].get("value")

        try:
            assert value is not None, "'value' must be specified"
            assert (
                isinstance(value, int) and value >= 0
            ), "`value` must be an integer greater than or equal to zero"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        # TODO: <Alex>ALEX</Alex>
        # print(f"\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._validate()] AVAILABLE_METRICS:\n{metrics} ; TYPE: {str(type(metrics))}")
        metrics = convert_to_json_serializable(data=metrics)
        # print(f"\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._validate()] AVAILABLE_METRICS-JSON_SERIALIZED:\n{metrics} ; TYPE: {str(type(metrics))}")
        # TODO: <Alex>ALEX</Alex>
        query_result = metrics.get("query.table")
        # print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT-0:\n{query_result} ; TYPE: {str(type(query_result))}')
        query_result = query_result[0]
        # print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT-1:\n{query_result} ; TYPE: {str(type(query_result))}')
        # query_result = metrics.get("query.table")[0][0]
        query_result = list(metrics.get("query.table")[0].values())[0]
        # print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT-2:\n{query_result} ; TYPE: {str(type(query_result))}')
        value = configuration["kwargs"].get("value")
        # TODO: <Alex>ALEX</Alex>
        # for element in query_result:
        #     print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT_ELEMENT:\n{element} ; TYPE: {str(type(element))}')
        #     print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT_ELEMENT_MAPPING:\n{element._mapping} ; TYPE: {str(type(element._mapping))}')
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # query_result = dict(query_result)
        # TODO: <Alex>ALEX</Alex>
        # TODO: <Alex>ALEX</Alex>
        # query_result_list = []
        # for element in query_result:
        #     print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT_ELEMENT:\n{element} ; TYPE: {str(type(element))}')
        #     print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT_ELEMENT.VALUES:\n{element.values()} ; TYPE: {str(type(element.values()))}')
        #     query_result_list.append(element.values())
        # print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT-1:\n{query_result_list} ; TYPE: {str(type(query_result))}')
        # query_result = dict(query_result_list)
        # TODO: <Alex>ALEX</Alex>
        # print(f'\n[ALEX_TEST] [ExpectQueriedTableRowCountToBe._VALIDATE()] QUERY_RESULT-2:\n{query_result} ; TYPE: {str(type(query_result))}')
        # TODO: <Alex>ALEX</Alex>
        # query_result = {
        #     element[0][0]: element[0][1]
        #     for element in query_result
        # }
        # TODO: <Alex>ALEX</Alex>

        success = query_result == value

        return {
            "success": success,
            "result": {"observed_value": query_result},
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": ["a", "a", "b", "b", "a"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "positive_test_static_data_asset",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                        "query": """
                                 SELECT COUNT(*)
                                 FROM test
                                 """,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "positive_test_row_condition",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                        "row_condition": 'col("col1")==2',
                        "condition_parser": "great_expectations__experimental__",
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectQueriedTableRowCountToBe().print_diagnostic_checklist()
