from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    import pandas as pd


class ColumnMedian(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.median"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Median Implementation"""
        column_null_elements_cond: pd.Series = column.isnull()
        column_nonnull_elements: pd.Series = column[~column_null_elements_cond]
        return column_nonnull_elements.median()

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
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
        """SqlAlchemy Median Implementation"""
        nonnull_count = metrics.get("column_values.nonnull.count")
        if not nonnull_count:
            return None

        element_values = execution_engine.execute_query(
            sa.select(column)
            .order_by(column)
            .where(column != None)  # noqa: E711
            .offset(max(nonnull_count // 2 - 1, 0))
            .limit(2)
            .select_from(selectable)
        )

        column_values = list(element_values.fetchall())

        if len(column_values) == 0:
            column_median = None
        elif nonnull_count % 2 == 0:
            # An even number of column values: take the average of the two center values
            column_median = (
                float(
                    column_values[0][0]
                    + column_values[1][0]  # left center value  # right center value
                )
                / 2.0
            )  # Average center values
        else:
            # An odd number of column values, we can just take the center value
            if len(column_values) == 1:  # noqa: PLR5501
                column_median = column_values[0][0]  # The only value
            else:
                column_median = column_values[1][0]  # True center value

        return column_median

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column = accessor_domain_kwargs["column"]
        # We will get the two middle values by choosing an epsilon to add
        # to the 50th percentile such that we always get exactly the middle two values
        # (i.e. 0 < epsilon < 1 / (2 * values))

        # Note that this can be an expensive computation; we are not exposing
        # spark's ability to estimate.
        # We add two to 2 * n_values to maintain a legitimate quantile
        # in the degenerate case when n_values = 0

        """Spark Median Implementation"""
        table_row_count = metrics.get("table.row_count")
        result = df.approxQuantile(
            column, [0.5, 0.5 + (1 / (2 + (2 * table_row_count)))], 0
        )
        return np.mean(result)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """This should return a dictionary:
        {
          "dependency_name": MetricConfiguration,
          ...
        }
        """
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies["column_values.nonnull.count"] = MetricConfiguration(
                metric_name="column_values.nonnull.count",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )

        return dependencies
