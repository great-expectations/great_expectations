"""
This is a template for creating custom QueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

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


class ExpectQueryCountWithFilterToMeetThreshold(QueryExpectation):
    """Expect Query given filter to contain at least as many entries as a given threshold."""

    metric_dependencies = ("query.template_values",)

    query = """
                SELECT COUNT(*) n
                FROM {active_batch}
                WHERE {col} = {filter}
            """

    success_keys = ("template_dict", "query")

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
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        threshold = configuration["kwargs"].get("threshold")

        try:
            assert threshold is not None, "'threshold' must be specified"
            assert isinstance(
                threshold, (int, float)
            ), "'threshold' must be a valid float or int"
            assert threshold > 0, "'threshold' must be positive"
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
        count: int = list(metrics.get("query.template_values")[0].values())[0]
        threshold: Union[float, int] = configuration["kwargs"].get("threshold")

        return {
            "success": count >= threshold,
            "result": {"observed_value": count},
        }

    examples = [
        {
            "data": [
                {
                    "data": {"col1": [1, 1, 1, 2, 2, 2, 2, 2]},
                },
            ],
            "suppress_test_for": ["bigquery"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {"col": "col1", "filter": 2},
                        "threshold": 4,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {"col": "col1", "filter": 1},
                        "threshold": 4,
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@CarstenFrommhold", "@mkopec87"],
    }


if __name__ == "__main__":
    ExpectQueryCountWithFilterToMeetThreshold().print_diagnostic_checklist()
