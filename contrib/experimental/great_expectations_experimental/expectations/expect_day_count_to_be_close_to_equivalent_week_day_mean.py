from datetime import datetime, timedelta
from typing import Dict, List, Optional

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import ColumnAggregateMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_value

TODAY_EXAMPLE: datetime = datetime(year=2022, month=8, day=10)
TODAY_EXAMPLE_STR: str = datetime.strftime(TODAY_EXAMPLE, "%Y-%m-%d")
date_format = "%Y-%m-%d"

METRIC_SAMPLE_LIMIT = 60

FOUR_PREVIOUS_WEEKS = [7, 14, 21, 28]


def get_days_ago_dict(current_date):
    return {
        3: current_date - timedelta(days=3),
        FOUR_PREVIOUS_WEEKS[0]: current_date - timedelta(days=FOUR_PREVIOUS_WEEKS[0]),
        FOUR_PREVIOUS_WEEKS[1]: current_date - timedelta(days=FOUR_PREVIOUS_WEEKS[1]),
        FOUR_PREVIOUS_WEEKS[2]: current_date - timedelta(days=FOUR_PREVIOUS_WEEKS[2]),
        FOUR_PREVIOUS_WEEKS[3]: current_date - timedelta(days=FOUR_PREVIOUS_WEEKS[3]),
    }


def generate_data_sample(n_appearances: dict):
    data = []
    for d, n in n_appearances.items():
        while n > 0:
            data.append(d)
            n -= 1
    return data


class ColumnCountsPerDaysCustom(ColumnAggregateMetricProvider):
    """
    This metric expects daily counts of the given column, to be close to the average counts calculated 4 weeks back,
    respective to the specific day of the week.
    The expectation fails if the difference in percentage ((current - average) / average) is more than the threshold
    given by user (default value is 25%). The threshold parameter should be given in fraction and not percent,
    i.e. for 25% define threshold = 0.25
    """

    metric_name = "column.counts_per_days_custom"

    library_metadata = {"tags": ["query-based"], "contributors": ["@itaise", "@hadasm"]}

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)

        # get counts for dates
        query = (
            sa.select([sa.func.Date(column), sa.func.count()])
            .group_by(sa.func.Date(column))
            .select_from(selectable)
            .order_by(sa.func.Date(column).desc())
            .limit(METRIC_SAMPLE_LIMIT)
        )
        results = execution_engine.execute_query(query).fetchall()
        return results


class ExpectDayCountToBeCloseToEquivalentWeekDayMean(ColumnAggregateExpectation):
    """Expect No missing days in date column"""

    # Default values
    default_kwarg_values = {"threshold": 0.25}
    example_days_ago_dict = get_days_ago_dict(TODAY_EXAMPLE)
    examples = [
        {
            # column a - good counts - 3 rows for every day
            "data": {
                "column_a": generate_data_sample(
                    {
                        TODAY_EXAMPLE: 3,
                        example_days_ago_dict[7]: 3,
                        example_days_ago_dict[14]: 3,
                        example_days_ago_dict[21]: 3,
                        example_days_ago_dict[28]: 3,
                    }
                ),
                "column_b": generate_data_sample(
                    {
                        TODAY_EXAMPLE: 2,
                        example_days_ago_dict[7]: 4,
                        example_days_ago_dict[14]: 3,
                        example_days_ago_dict[21]: 3,
                        example_days_ago_dict[28]: 3,
                    }
                ),
                "column_datetime": generate_data_sample(
                    {
                        TODAY_EXAMPLE: 3,
                        example_days_ago_dict[7]: 2,
                        example_days_ago_dict[7].replace(hour=11): 1,
                        example_days_ago_dict[14]: 2,
                        example_days_ago_dict[14].replace(hour=10, minute=40): 1,
                        example_days_ago_dict[21]: 3,
                        example_days_ago_dict[28]: 3,
                    }
                ),
                "column_current_zero": generate_data_sample(
                    {
                        TODAY_EXAMPLE: 0,
                        example_days_ago_dict[7]: 4,
                        example_days_ago_dict[14]: 4,
                        example_days_ago_dict[21]: 4,
                        example_days_ago_dict[28]: 3,
                    }
                ),
                "column_past_mean_zero": generate_data_sample(
                    {
                        TODAY_EXAMPLE: 15,
                        example_days_ago_dict[7]: 0,
                        example_days_ago_dict[14]: 0,
                        example_days_ago_dict[21]: 0,
                        example_days_ago_dict[28]: 0,
                    }
                ),
            },
            "only_for": ["sqlite"],
            "tests": [
                {
                    "title": "positive test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_a",
                        "run_date": TODAY_EXAMPLE_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": True},
                },
                {
                    "title": "positive test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_datetime",
                        "run_date": TODAY_EXAMPLE_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": True},
                },
                {
                    "title": "positive test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_datetime",
                        "run_date": TODAY_EXAMPLE_STR,
                        "threshold": default_kwarg_values["threshold"],
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_b",
                        "run_date": TODAY_EXAMPLE_STR,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_current_zero",
                        "run_date": TODAY_EXAMPLE_STR,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_past_mean_zero",
                        "run_date": TODAY_EXAMPLE_STR,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    metric_dependencies = ("column.counts_per_days_custom",)
    success_keys = (
        "run_date",
        "threshold",
    )

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
        run_date_str = self.get_success_kwargs(configuration).get("run_date")

        run_date = datetime.strptime(run_date_str, date_format)

        threshold = float(self.get_success_kwargs(configuration).get("threshold"))

        days_ago_dict = get_days_ago_dict(run_date)

        equivalent_previous_days: List[datetime] = [
            days_ago_dict[i] for i in FOUR_PREVIOUS_WEEKS
        ]

        assert min(equivalent_previous_days) > (
            datetime.today() - timedelta(METRIC_SAMPLE_LIMIT)
        ), (
            f"Data includes only up to {METRIC_SAMPLE_LIMIT} days prior to today ({datetime.today()}), "
            f"but 4 weeks before the given run_date is {min(equivalent_previous_days)}",
        )

        day_counts_dict = get_counts_per_day_as_dict(
            metrics, run_date_str, equivalent_previous_days
        )
        run_date_count: int = day_counts_dict[run_date_str]
        diff_fraction = get_diff_fraction(
            run_date_count, day_counts_dict, equivalent_previous_days
        )

        if diff_fraction > threshold:
            msg = (
                f"The diff between yesterday's count and the avg. count ({diff_fraction:.0%}) exceeds the defined "
                f"threshold ({threshold:.0%})"
            )
            success = False
        else:
            msg = (
                f"The diff between yesterday's count ({run_date_count}) and the avg. count ({diff_fraction:.0%}) "
                f"is below threshold"
            )
            success = True

        return {"success": success, "result": {"details": msg}}


def get_counts_per_day_as_dict(
    metrics: dict, run_date: str, equivalent_previous_days: list
) -> dict:
    equivalent_previous_days_str: List[str] = [
        datetime.strftime(i, date_format) for i in equivalent_previous_days
    ]
    all_days_list = equivalent_previous_days_str + [run_date]

    counts_per_days = metrics["column.counts_per_days_custom"]
    day_counts_dict = {i[0]: i[1] for i in counts_per_days}

    for day in all_days_list:
        if day not in day_counts_dict.keys():
            day_counts_dict.update({day: 0})

    return day_counts_dict


def get_diff_fraction(
    run_date_count: int, day_counts_dict: dict, equivalent_previous_days: list
) -> float:
    """
    Calculates the fractional difference between current and past average row counts (how much is the
    difference relative to the average).
    Added +1 to both nuemrator and denominator, to account for cases when previous average is 0.
    """

    equivalent_previous_days_str: List[str] = [
        datetime.strftime(i, date_format) for i in equivalent_previous_days
    ]

    previous_days_counts: List[int] = [
        day_counts_dict[i] for i in day_counts_dict if i in equivalent_previous_days_str
    ]

    avg_equivalent_previous_days_count = average_if_nonempty(previous_days_counts)

    absolute_diff = abs(run_date_count - avg_equivalent_previous_days_count)
    return (1 + absolute_diff) / (1 + avg_equivalent_previous_days_count)


def average_if_nonempty(list_: list):
    return sum(list_) / len(list_) if len(list_) > 0 else 0


if __name__ == "__main__":
    ExpectDayCountToBeCloseToEquivalentWeekDayMean().print_diagnostic_checklist()
