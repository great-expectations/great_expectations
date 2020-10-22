from typing import Optional

import numpy as np

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnAggregateMetric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import F as F
from great_expectations.expectations.metrics.column_aggregate_metric import (
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnMedian(ColumnAggregateMetric):
    """Metric Class for Aggregate Mean Metric"""

    metric_name = "column.aggregate.median"

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Median Implementation"""
        return column.median()

    @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _table, _dialect, _engine, _metrics, **kwargs):
        """SqlAlchemy Median Implementation"""
        if _dialect.name.lower() == "awsathena":
            raise NotImplementedError("AWS Athena does not support OFFSET.")
        nonnull_count = _metrics.get("column_values.nonnull.count")
        element_values = _engine.execute(
            sa.select([column])
            .order_by(column)
            .where(column != None)
            .offset(max(nonnull_count // 2 - 1, 0))
            .limit(2)
            .select_from(_table)
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
            column_median = column_values[1][0]  # True center value
        return column_median

    @column_aggregate_metric(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _metrics, **kwargs):
        """Spark Median Implementation"""
        table_row_count = _metrics.get("table.row_count")
        result = _table.approxQuantile(
            column, [0.5, 0.5 + (1 / (2 + (2 * table_row_count)))], 0
        )
        return np.mean(result)

    @classmethod
    def get_evaluation_dependencies(
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
        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }

        return {
            "column_values.nonnull.count": MetricConfiguration(
                "column_values.nonnull.count", metric.metric_domain_kwargs
            ),
            "table.row_count": MetricConfiguration(
                "table.row_count", table_domain_kwargs
            ),
        }
