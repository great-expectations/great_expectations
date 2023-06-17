from datetime import datetime, timedelta
from typing import Dict, List, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricFunctionTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import ColumnAggregateMetricProvider
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.metric_provider import metric_value

TODAY: datetime = datetime(year=2022, month=8, day=10)
TODAY_STR: str = datetime.strftime(TODAY, "%Y-%m-%d")
date_format = "%Y-%m-%d"

DAYS_AGO = {
    3: TODAY - timedelta(days=3),
    7: TODAY - timedelta(days=7),
    14: TODAY - timedelta(days=14),
    21: TODAY - timedelta(days=21),
    28: TODAY - timedelta(days=28),
}

DAYS_IN_WEEK = 7

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

    @metric_value(
        engine=SqlAlchemyExecutionEngine,
        metric_fn_type=MetricFunctionTypes.AGGREGATE_VALUE,
        domain_type=MetricDomainTypes.COLUMN,
    )
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
        sqlalchemy_engine = execution_engine.engine

        # get counts for dates
        query = (
            sa.select([sa.func.Date(column), sa.func.count()])
            .group_by(column)
            .select_from(selectable)
            .order_by(column.desc())
            .limit(30)
        )
        results = sqlalchemy_engine.execute(query).fetchall()
        return results


class ExpectDayCountToBeCloseToEquivalentWeekDayMean(ColumnExpectation):
    """Expect No missing days in date column


    Keyword Args:
        - threshold (float between 0-1, default is 0.25): expectation fails if the difference in percentage is more than the threshold.
        - weeks_back (int): how many weeks back the comparison goes

    See Also:
        [expect_day_sum_to_be_close_to_equivalent_week_day_mean](https://greatexpectations.io/expectations/expect_day_sum_to_be_close_to_equivalent_week_day_mean)
    """

    # Default values
    default_kwarg_values = {"threshold": 0.25, "weeks_back": 4}

    examples = [
        {
            # column a - good counts - 3 rows for every day
            "data": {
                "column_a": generate_data_sample(
                    {
                        TODAY: 3,
                        DAYS_AGO[7]: 3,
                        DAYS_AGO[14]: 3,
                        DAYS_AGO[21]: 3,
                        DAYS_AGO[28]: 3,
                    }
                ),
                "column_b": generate_data_sample(
                    {
                        TODAY: 2,
                        DAYS_AGO[7]: 4,
                        DAYS_AGO[14]: 3,
                        DAYS_AGO[21]: 3,
                        DAYS_AGO[28]: 3,
                    }
                ),
                "column_current_zero": generate_data_sample(
                    {
                        TODAY: 0,
                        DAYS_AGO[7]: 4,
                        DAYS_AGO[14]: 4,
                        DAYS_AGO[21]: 4,
                        DAYS_AGO[28]: 3,
                    }
                ),
                "column_past_mean_zero": generate_data_sample(
                    {
                        TODAY: 15,
                        DAYS_AGO[7]: 0,
                        DAYS_AGO[14]: 0,
                        DAYS_AGO[21]: 0,
                        DAYS_AGO[28]: 0,
                    }
                ),
            },
            # "column_b": [today, yesterday, yesterday, two_days_ago]},
            "tests": [
                {
                    "title": "positive test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_a",
                        "run_date": TODAY_STR,
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
                        "run_date": TODAY_STR,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_current_zero",
                        "run_date": TODAY_STR,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "negative test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {
                        "column": "column_past_mean_zero",
                        "run_date": TODAY_STR,
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
        "week_back"
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

        success_kwargs = self.get_success_kwargs(configuration)
        run_date: str = success_kwargs.get("run_date")
        threshold: float = float(success_kwargs.get("threshold"))
        weeks_back: int = success_kwargs.get("weeks_back")

        days_back_list = [DAYS_IN_WEEK*week_index for week_index in range(1, weeks_back+1)]

        day_counts_dict = get_counts_per_day_as_dict(metrics, run_date, days_back_list)
        run_date_count: int = day_counts_dict[run_date]

        diff_fraction = get_diff_fraction(run_date_count, day_counts_dict, days_back_list)

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


def get_counts_per_day_as_dict(metrics: dict, run_date: str, days_back_list: List[int]) -> dict:
    equivalent_previous_days: List[datetime] = [
        DAYS_AGO[i] for i in days_back_list
    ]
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


def get_diff_fraction(run_date_count: int, day_counts_dict: dict, days_back_list: List[int]) -> float:
    """
    Calculates the fractional difference between current and past average row counts (how much is the
    difference relative to the average).
    Added +1 to both nuemrator and denominator, to account for cases when previous average is 0.
    """
    equivalent_previous_days: List[datetime] = [
        DAYS_AGO[i] for i in days_back_list
    ]
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
