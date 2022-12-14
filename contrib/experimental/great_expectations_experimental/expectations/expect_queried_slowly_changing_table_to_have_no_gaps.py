from datetime import datetime, timedelta
from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedSlowlyChangingTableToHaveNoGaps(QueryExpectation):
    """Expect Slowly changing table type II to have no gaps between the 'end date' of each row, and the next 'start date' in the next row.

    Args:
        template_dict: dict with the following keys: \
            primary_key (primary key column name or multiple columns, comma separated), \
            open_date_column (name of the column representing open date), \
            close_date_column (name of the column representing clode date)
        threshold: an optional parameter - default is zero. \
            if the ratio of "gaps" to total table rows is higher than threshold - error will be raised.
    """

    metric_dependencies = ("query.template_values",)

    query = """
    SELECT SUM(CASE WHEN {close_date_column} != COALESCE(next_start_date, {close_date_column}) THEN 1 ELSE 0 END), 
    COUNT(1)
    FROM(SELECT {primary_key}, {close_date_column}, LEAD({open_date_column}) OVER(PARTITION BY {primary_key} ORDER BY 
    {open_date_column}) AS next_start_date 
    FROM {active_batch})
    """

    success_keys = (
        "template_dict",
        "threshold",
        "query",
    )

    domain_keys = (
        "template_dict",
        "query",
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "threshold": 0,
        "query": query,
    }

    library_metadata = {"tags": ["query-based"], "contributors": ["@itaise"]}

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:

        success = False
        threshold = configuration["kwargs"].get("threshold")
        if not threshold:
            threshold = self.default_kwarg_values["threshold"]
        query_result = metrics.get("query.template_values")
        holes_count, total_count = query_result[0]
        error_rate = holes_count / total_count

        if error_rate <= threshold:
            success = True

        return {
            "success": success,
            "result": {
                "threshold": threshold,
                "holes_count": holes_count,
                "total_count": total_count,
            },
        }

    today = datetime(year=2022, month=8, day=10)
    one_day_ago = today - timedelta(days=1)
    two_day_ago = today - timedelta(days=2)
    three_day_ago = today - timedelta(days=3)
    four_day_ago = today - timedelta(days=4)
    five_day_ago = today - timedelta(days=5)
    six_day_ago = today - timedelta(days=6)
    seven_day_ago = today - timedelta(days=7)
    eight_day_ago = today - timedelta(days=8)
    nine_day_ago = today - timedelta(days=9)
    ten_day_ago = today - timedelta(days=10)

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "msid": [
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                        ],
                        "uuid": [
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                        ],
                        "col1": [1, 2, 2, 3, 4, 5, 6, 7, 8],
                        "col2": ["a", "a", "b", "b", "a", "a", "a", "a", "a"],
                        "start_date": [
                            ten_day_ago,
                            nine_day_ago,
                            eight_day_ago,
                            seven_day_ago,
                            six_day_ago,
                            five_day_ago,
                            four_day_ago,
                            three_day_ago,
                            two_day_ago,
                        ],
                        "end_date": [
                            nine_day_ago,
                            eight_day_ago,
                            seven_day_ago,
                            six_day_ago,
                            five_day_ago,
                            four_day_ago,
                            three_day_ago,
                            two_day_ago,
                            one_day_ago,
                        ],
                        "start_date_2": [
                            ten_day_ago,
                            seven_day_ago,
                            six_day_ago,
                            five_day_ago,
                            four_day_ago,
                            three_day_ago,
                            two_day_ago,
                            two_day_ago,
                            two_day_ago,
                        ],
                        "end_date_2": [
                            nine_day_ago,
                            six_day_ago,
                            six_day_ago,
                            five_day_ago,
                            four_day_ago,
                            three_day_ago,
                            two_day_ago,
                            two_day_ago,
                            two_day_ago,
                        ],
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
                            "primary_key": "msid,uuid",
                            "open_date_column": "start_date",
                            "close_date_column": "end_date",
                        }
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "primary_key": "msid,uuid",
                            "open_date_column": "start_date_2",
                            "close_date_column": "end_date_2",
                        },
                        "threshold": 0.1,
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite"],
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                }
            ],
        },
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        threshold = configuration["kwargs"].get("threshold")
        if not threshold:
            threshold = self.default_kwarg_values["threshold"]

        try:
            assert isinstance(threshold, int) or isinstance(threshold, float)
            assert threshold >= 0
            assert threshold <= 1

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


if __name__ == "__main__":
    ExpectQueriedSlowlyChangingTableToHaveNoGaps().print_diagnostic_checklist()
