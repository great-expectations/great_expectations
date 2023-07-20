from typing import Dict, Optional

from dateutil.relativedelta import relativedelta

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

MONTH_FORMAT = "%Y-%m"


class ColumnDistinctMonths(ColumnAggregateMetricProvider):
    """Metric that get all unique months from date column"""

    metric_name = "column.distinct_months"

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

        # get all unique months from timestamp
        query = sa.select(
            sa.func.strftime(MONTH_FORMAT, sa.func.Date(column)).distinct()
        ).select_from(selectable)
        all_unique_months = [
            i[0] for i in execution_engine.execute_query(query).fetchall()
        ]

        return all_unique_months


class ExpectColumnToHaveNoMonthsMissing(ColumnAggregateExpectation):
    """
    This metric expects data to include dates from consecutive months, with no months missing in
    between (relative to the maximal and minimal month existing in the data).
    User can define a threshold which allows for some months to be missing (equal or lower than the threshold).
    Expectation fails if the number of missing months is over the threshold.

    Keyword args:
        - threshold (int)
    """

    from datetime import datetime

    today = datetime.now()
    months_ago = {
        1: today - relativedelta(months=1),
        2: today - relativedelta(months=2),
        3: today - relativedelta(months=3),
        4: today - relativedelta(months=4),
    }

    examples = [
        {
            "data": {
                "column_two_months_missing": [
                    today,
                    months_ago[1],
                    months_ago[1],
                    months_ago[4],
                ],
                "column_one_month_missing": [
                    today,
                    months_ago[2],
                    months_ago[2],
                    months_ago[2],
                ],
                "column_none_missing": [
                    today,
                    months_ago[1],
                    months_ago[1],
                    months_ago[2],
                ],
            },
            "suppress_test_for": ["mssql"],
            "tests": [
                {
                    "title": "negative_missing_two_months",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_two_months_missing", "threshold": 1},
                    "out": {"success": False},
                },
                {
                    "title": "positive_missing_two_months",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_two_months_missing", "threshold": 4},
                    "out": {"success": True},
                },
                {
                    "title": "negative_missing_one_month",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_one_month_missing", "threshold": 0},
                    "out": {"success": False},
                },
                {
                    "title": "positive_none_missing",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "column_none_missing", "threshold": 2},
                    "out": {"success": True},
                },
            ],
        }
    ]

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("column.distinct_months",)
    success_keys = ("threshold",)

    # Default values
    default_kwarg_values = {}

    library_metadata = {
        "maturity": "experimental",
        "contributors": [
            "@hadasm",
        ],
        "tags": ["date-column"],
    }

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
        from datetime import datetime

        dist_months_as_str = metrics["column.distinct_months"]
        distinct_months_sorted = sorted(
            [
                datetime.strptime(month_str, MONTH_FORMAT)
                for month_str in dist_months_as_str
            ]
        )
        min_month, max_month = distinct_months_sorted[0], distinct_months_sorted[-1]
        months_diff = relativedelta(max_month, min_month).months
        month_set = {
            min_month + relativedelta(months=n_month) for n_month in range(months_diff)
        }
        n_missing_months = len(month_set - set(distinct_months_sorted))

        threshold = self.get_success_kwargs(configuration).get("threshold")
        success: bool = n_missing_months <= threshold
        return {
            "success": success,
            "result": {
                "Number of missing months": n_missing_months,
                "Total unique months": len(distinct_months_sorted),
                "Threshold": threshold,
                "Min date": min_month,
                "Max date": max_month,
            },
        }


if __name__ == "__main__":
    ExpectColumnToHaveNoMonthsMissing().print_diagnostic_checklist()
