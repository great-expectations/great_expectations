from typing import Any, Dict, List, Optional, Set

import pandas as pd

from great_expectations.compatibility import pyspark, sqlalchemy
from great_expectations.compatibility.pyspark import (
    functions as F,
)
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
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnDistinctValues(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> Set[Any]:
        return set(column.unique())

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict[str, str],
        **kwargs,
    ) -> Set[Any]:
        """
        Past implementations of column.distinct_values depended on column.value_counts.
        This was causing performance issues due to the complex query used in column.value_counts and subsequent
        in-memory operations.
        """
        selectable: sqlalchemy.Selectable
        accessor_domain_kwargs: Dict[str, str]
        (
            selectable,
            _,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column_name: str = accessor_domain_kwargs["column"]
        column: sqlalchemy.ColumnClause = sa.column(column_name)

        distinct_values: List[sqlalchemy.Row]
        if hasattr(column, "is_not"):
            distinct_values = execution_engine.execute_query(
                sa.select(column)
                .where(column.is_not(None))
                .distinct()
                .select_from(selectable)
            ).fetchall()
        else:
            distinct_values = execution_engine.execute_query(
                sa.select(column)
                .where(column.isnot(None))
                .distinct()
                .select_from(selectable)
            ).fetchall()
        # Vectorized operation is not faster here due to overhead of converting to and from numpy array
        return {row[0] for row in distinct_values}

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict[str, str],
        **kwargs,
    ) -> Set[Any]:
        """
        Past implementations of column.distinct_values depended on column.value_counts.
        This was causing performance issues due to the complex query used in column.value_counts and subsequent
        in-memory operations.
        """
        df: pyspark.DataFrame
        accessor_domain_kwargs: Dict[str, str]
        (
            df,
            _,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column_name: str = accessor_domain_kwargs["column"]
        distinct_values: List[pyspark.Row] = (
            df.select(F.col(column_name))
            .distinct()
            .where(F.col(column_name).isNotNull())
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        return set(distinct_values)


class ColumnDistinctValuesCount(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, **kwargs) -> int:
        return column.nunique()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column: sqlalchemy.ColumnClause,
        **kwargs,
    ) -> sqlalchemy.Selectable:
        """
        Past implementations of column.distinct_values.count depended on column.value_counts and column.distinct_values.
        This was causing performance issues due to the complex query used in column.value_counts and subsequent
        in-memory operations.
        """
        return sa.func.count(sa.distinct(column))

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column: pyspark.Column,
        **kwargs,
    ) -> pyspark.Column:
        """
        Past implementations of column.distinct_values.count depended on column.value_counts and column.distinct_values.
        This was causing performance issues due to the complex query used in column.value_counts and subsequent
        in-memory operations.
        """
        return F.countDistinct(column)


class ColumnDistinctValuesCountUnderThreshold(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count.under_threshold"
    condition_keys = ("threshold",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: pd.Series, threshold: int, **kwargs) -> bool:
        return column.nunique() < threshold

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        metric_value_kwargs: Dict[str, int],
        metrics: Dict[str, int],
        **kwargs,
    ) -> bool:
        return (
            metrics["column.distinct_values.count"] < metric_value_kwargs["threshold"]
        )

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        metric_value_kwargs: Dict[str, int],
        metrics: Dict[str, int],
        **kwargs,
    ) -> bool:
        return (
            metrics["column.distinct_values.count"] < metric_value_kwargs["threshold"]
        )

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[Dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration,
        specifying the metric types and their respective domains"""
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        if metric.metric_name == "column.distinct_values.count.under_threshold":
            dependencies["column.distinct_values.count"] = MetricConfiguration(
                metric_name="column.distinct_values.count",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )
        return dependencies
