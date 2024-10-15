from typing import Dict

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import ColumnAggregateMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_value


class ColumnDistinctDates(ColumnAggregateMetricProvider):
    """Metric that get all unique dates from date column (excluding null values)"""

    metric_name = "column.distinct_dates"

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        from datetime import date

        (
            selectable,
            _compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(metric_domain_kwargs, MetricDomainTypes.COLUMN)

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)

        # get all unique dates from timestamp (excluding null values)
        query = (
            sa.select(sa.func.Date(column).distinct())
            .select_from(selectable)
            .where(column.is_not(None))
        )

        all_unique_dates = [i[0] for i in execution_engine.execute_query(query).fetchall()]

        # Only sqlite returns as strings, so make date objects be strings
        if all_unique_dates and isinstance(all_unique_dates[0], date):
            all_unique_dates = [
                unique_date.strftime("%Y-%m-%d") for unique_date in all_unique_dates
            ]
        return all_unique_dates


class ExpectColumnToHaveNoDaysMissing(ColumnAggregateExpectation):
    """Expect No missing days in date column."""

    from datetime import datetime, timedelta

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)
    thirty_days_ago = today - timedelta(days=30)
    sixty_days_ago = today - timedelta(days=60)

    examples = [
        {
            "data": {
                "column_a": [today, yesterday, thirty_days_ago, sixty_days_ago],
                "column_b": [today, yesterday, yesterday, two_days_ago],
                "column_c": [today, two_days_ago, two_days_ago, two_days_ago],
            },
            "suppress_test_for": ["mssql"],
            "tests": [
                {
                    "title": "missing_many_days",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_a", "threshold": 4},
                    "out": {"success": False},
                },
                {
                    "title": "missing_one_day",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_c", "threshold": 1},
                    "out": {"success": True},
                },
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_b", "threshold": 2},
                    "out": {"success": True},
                },
            ],
        }
    ]

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("column.distinct_dates",)
    success_keys = ("threshold",)

    # Default values
    default_kwarg_values = {}

    library_metadata = {
        "maturity": "experimental",
        "contributors": [
            "@itaise",
        ],
        "tags": ["date-column"],
    }

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        from datetime import datetime, timedelta

        # returns the distinct dates of the column
        dist_dates_as_str = metrics["column.distinct_dates"]
        distinct_dates_sorted = sorted(
            [datetime.strptime(date_str, "%Y-%m-%d") for date_str in dist_dates_as_str]
        )
        min_date, max_date = distinct_dates_sorted[0], distinct_dates_sorted[-1]
        days_diff = (max_date - min_date).days
        date_set = {distinct_dates_sorted[0] + timedelta(x) for x in range(days_diff)}
        missing_days = sorted(date_set - set(distinct_dates_sorted))

        threshold = self._get_success_kwargs().get("threshold")
        success: bool = len(missing_days) <= threshold
        return {
            "success": success,
            "result": {
                "Number of missing days": len(missing_days),
                "Total unique days": len(distinct_dates_sorted),
                "Threshold": threshold,
                "Min date": min_date,
                "Max date": max_date,
            },
        }


if __name__ == "__main__":
    ExpectColumnToHaveNoDaysMissing().print_diagnostic_checklist()
