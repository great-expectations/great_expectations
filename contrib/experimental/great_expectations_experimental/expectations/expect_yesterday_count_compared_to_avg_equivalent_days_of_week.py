from datetime import date, datetime, timedelta
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

DAYS_AGO = {
    3: TODAY - timedelta(days=3),
    7: TODAY - timedelta(days=7),
    14: TODAY - timedelta(days=14),
    21: TODAY - timedelta(days=21),
    28: TODAY - timedelta(days=28),
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

        # Only sqlite returns as strings, so make date objects be strings
        if results and isinstance(results[0][0], date):
            results = [
                (result[0].strftime("%Y-%m-%d"), result[1]) for result in results
            ]

        return results


class ExpectYesterdayCountComparedToAvgEquivalentDaysOfWeek(ColumnExpectation):
    """Expect No missing days in date column"""

    # Default values
    default_kwarg_values = {"threshold": 0.25}

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
            },
            # "column_b": [today, yesterday, yesterday, two_days_ago]},
            "suppress_test_for": ["mssql"],
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
        date_format = "%Y-%m-%d"
        from datetime import datetime, timedelta

        counts_per_days = metrics["column.counts_per_days_custom"]
        run_date: str = self.get_success_kwargs(configuration).get("run_date")
        threshold: float = float(
            self.get_success_kwargs(configuration).get("threshold")
        )
        run_date_as_date: datetime = datetime.strptime(run_date, date_format)
        equivalent_days_in_previous_weeks: List[datetime] = [
            run_date_as_date - timedelta(delta) for delta in [7, 14, 21, 28]
        ]
        equivalent_days_in_previous_weeks_str: List[str] = [
            datetime.strftime(i, date_format) for i in equivalent_days_in_previous_weeks
        ]

        previous_days_counts: List[int] = [
            i[1]
            for i in counts_per_days
            if i[0] in equivalent_days_in_previous_weeks_str
        ]
        yesterday_count: int = [i[1] for i in counts_per_days if i[0] == run_date][0]

        avg_equivalent_previous_days_count = 0
        if len(previous_days_counts) > 0:
            avg_equivalent_previous_days_count = sum(previous_days_counts) / len(
                previous_days_counts
            )

        absolute_diff = abs(yesterday_count - avg_equivalent_previous_days_count)
        diff_percentage = (
            absolute_diff / avg_equivalent_previous_days_count
            if avg_equivalent_previous_days_count > 0
            else 1
        )

        if diff_percentage > threshold:
            msg = (
                f"The diff between yesterday's count and the avg. count ({diff_percentage:.0%}) exceeds the defined "
                f"threshold ({threshold:.0%})"
            )
            success = False
        else:
            msg = (
                f"The diff between yesterday's count ({yesterday_count}) and the avg. count ({diff_percentage:.0%}) "
                f"is below threshold"
            )
            success = True

        return {"success": success, "result": {"details": msg}}


if __name__ == "__main__":
    ExpectYesterdayCountComparedToAvgEquivalentDaysOfWeek().print_diagnostic_checklist()
