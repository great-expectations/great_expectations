from typing import Any, Dict, Optional, Set

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import metric_value


class ColumnDistinctValues(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs) -> Set[Any]:
        return set(column.unique())

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ) -> Set[Any]:
        selectable, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column = accessor_domain_kwargs["column"]

        query = sa.select(column).distinct()
        results = execution_engine.engine.execute(
            query.select_from(selectable)
        ).fetchall()

        return set(results)

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ) -> Set[Any]:
        df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column = accessor_domain_kwargs["column"]

        distinct_values = (
            df.select(column).where(F.col(column).isNotNull()).groupBy(column).collect()
        )

        return set(distinct_values)


class ColumnDistinctValuesCount(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: str, **kwargs: Optional[dict]) -> Set[Any]:
        return column.nunique()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column: str, **kwargs: Optional[dict]) -> Set[Any]:
        return sa.select(column).distinct().count()

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column: str,
        **kwargs: Optional[dict],
    ) -> Set[Any]:
        return F.select(column).where(F.col(column).isNotNull()).groupBy(column).count()


class ColumnDistinctValuesCountUnderThreshold(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count.under_threshold"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column: str, **kwargs: Optional[dict]) -> Set[Any]:
        return column.nunique()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column: str,
        **kwargs: Optional[dict],
    ) -> Set[Any]:
        return None

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column: str,
        **kwargs: Optional[dict],
    ) -> Set[Any]:
        return None
