from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import QueryExpectation

TODAY: date = datetime(year=2022, month=8, day=10).date()
TODAY_STR: str = datetime.strftime(TODAY, "%Y-%m-%d")

date_format = "%Y-%m-%d"

DAYS_IN_WEEK = 7


def generate_data_sample(n_appearances: dict):
    data = []
    for d, n in n_appearances.items():
        while n > 0:
            data.append(d)
            n -= 1
    return data


class ExpectDaySumToBeCloseToEquivalentWeekDayMean(QueryExpectation):
    """Expect the daily sums of the given column to be close to the average sums calculated 4 weeks back.

    This metric expects daily sums of the given column, to be close to the average sums calculated 4 weeks back,
    respective to the specific day of the week.
    The expectation fails if the difference in percentage ((current_sum - average_sum) / average_sum) is more than the
    threshold given by user (default value is 25%).
    The threshold parameter should be given in fraction and not percent, i.e. for 25% define threshold = 0.25.

    Keyword args:
        - threshold (float; default = 0.25): threshold of difference between current and past weeks over which expectation fails
        - weeks_back (int; default = 4): how many weeks back to compare the current metric with
    """

    FOUR_PREVIOUS_WEEKS = [7, 14, 21, 28]
    DAYS_AGO = {
        3: TODAY - timedelta(days=3),
        7: TODAY - timedelta(days=7),
        14: TODAY - timedelta(days=14),
        21: TODAY - timedelta(days=21),
        28: TODAY - timedelta(days=28),
    }

    query = """
    SELECT {date_column} as date_column, SUM({summed_column}) as column_sum_over_date
    FROM {active_batch}
    GROUP BY {date_column}
    """

    run_date = TODAY

    # Default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "threshold": 0.25,
        "query": query,
        "weeks_back": 4,
    }

    examples = [
        {
            # INFO: column a - good counts - 3 rows for every day
            "data": {
                "date_column_a": generate_data_sample(
                    {
                        TODAY: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[0]]: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[1]]: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[2]]: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[3]]: 3,
                    }
                ),
                "summed_column_a": generate_data_sample({1: 15}),
                "date_column_b": generate_data_sample(
                    {
                        TODAY: 2,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[0]]: 4,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[1]]: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[2]]: 3,
                        DAYS_AGO[FOUR_PREVIOUS_WEEKS[3]]: 3,
                    }
                ),
                "summed_column_b": generate_data_sample({1: 15}),
                "summed_column_zero_avg": generate_data_sample({1: 3, 0: 12}),
                "summed_column_zero_current": generate_data_sample({1: 3, 0: 12}),
                "summed_column_zero_both": generate_data_sample({1: 3, 0: 12}),
            },
            # INFO: "column_b": [today, yesterday, yesterday, two_days_ago]},
            "suppress_test_for": ["bigquery"],
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "summed_column": "summed_column_a",
                            "date_column": "date_column_a",
                        },
                        "run_date": TODAY_STR,
                        "threshold": default_kwarg_values["threshold"],
                        "weeks_back": default_kwarg_values["weeks_back"],
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "template_dict": {
                            "summed_column": "summed_column_b",
                            "date_column": "date_column_b",
                        },
                        "run_date": TODAY_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "template_dict": {
                            "summed_column": "summed_column_zero_avg",
                            "date_column": "date_column_a",
                        },
                        "run_date": TODAY_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "template_dict": {
                            "summed_column": "summed_column_zero_current",
                            "date_column": "date_column_a",
                        },
                        "run_date": TODAY_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "template_dict": {
                            "summed_column": "summed_column_zero_both",
                            "date_column": "date_column_a",
                        },
                        "run_date": TODAY_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    metric_dependencies = ("query.template_values",)

    success_keys = ("template_dict", "threshold", "query", "run_date", "weeks_back")

    domain_keys = (
        "template_dict",
        "query",
    )

    library_metadata = {"tags": ["query-based"], "contributors": ["@itaise", "@hadasm"]}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        # Setting up a configuration
        super().validate_configuration(configuration)

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        success_kwargs = self.get_success_kwargs(configuration)
        run_date: str = success_kwargs.get("run_date")
        threshold: float = float(success_kwargs.get("threshold"))
        weeks_back = success_kwargs.get("weeks_back")

        days_back_list = [
            DAYS_IN_WEEK * week_index for week_index in range(1, weeks_back + 1)
        ]

        result_dict = get_results_dict(metrics)

        yesterday_sum: int = result_dict[run_date]
        diff_fraction = get_diff_fraction(yesterday_sum, result_dict, days_back_list)

        if diff_fraction > threshold:
            msg = (
                f"The diff between yesterday's count and the avg. count ({diff_fraction:.0%}) exceeds the defined "
                f"threshold ({threshold:.0%})"
            )
            success = False
        else:
            msg = (
                f"The diff between yesterday's count ({yesterday_sum}) and the avg. count ({diff_fraction:.0%}) "
                f"is below threshold"
            )
            success = True

        return {"success": success, "result": {"details": msg}}


def get_results_dict(metrics: dict) -> dict:
    metrics = convert_to_json_serializable(data=metrics)
    result_list = metrics.get("query.template_values")
    result_dict = {}
    result_dict.update(
        {i["date_column"]: i["column_sum_over_date"] for i in result_list}
    )
    return result_dict


def average_if_nonempty(list_: list):
    return sum(list_) / len(list_) if len(list_) > 0 else 0


def get_diff_fraction(yesterday_sum: int, result_dict: dict, days_back_list: List[int]):
    days_ago_dict = {
        days_ago: TODAY - timedelta(days=days_ago) for days_ago in days_back_list
    }

    equivalent_previous_days: List[date] = list(days_ago_dict.values())
    equivalent_previous_days_str: List[str] = [
        datetime.strftime(i, date_format) for i in equivalent_previous_days
    ]
    previous_days_sums: List[int] = [
        result_dict[equiv_day] for equiv_day in equivalent_previous_days_str
    ]

    avg_equivalent_previous_days_sum = average_if_nonempty(previous_days_sums)
    absolute_diff = abs(yesterday_sum - avg_equivalent_previous_days_sum)
    return (absolute_diff + 1) / (avg_equivalent_previous_days_sum + 1)


if __name__ == "__main__":
    ExpectDaySumToBeCloseToEquivalentWeekDayMean().print_diagnostic_checklist()
