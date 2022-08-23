from typing import Any, Dict, Optional, Set

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
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


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

        query = sa.select(sa.column(column)).distinct()
        results = execution_engine.engine.execute(
            query.select_from(selectable)
        ).fetchall()

        return {row[0] for row in results}

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
            df.select(column)
            .distinct()
            .where(F.col(column).isNotNull())
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        return set(distinct_values)


class ColumnDistinctValuesCount(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.nunique()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.count(column.distinct())

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        **kwargs,
    ):
        return F.select(column).distinct().count()


class ColumnDistinctValuesCountUnderThreshold(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count.under_threshold"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs) -> Set[Any]:
        return column.nunique()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        metrics: Dict[str, Any],
        **kwargs,
    ) -> Set[Any]:
        column_distinct_values_count = metrics["column.distinct_values.count"]
        return column_distinct_values_count

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column: str,
        metrics: Dict[str, Any],
        **kwargs: Optional[dict],
    ) -> Set[Any]:
        column_distinct_values_count = metrics["column.distinct_values.count"]
        return column_distinct_values_count

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
        if isinstance(
            execution_engine, (SqlAlchemyExecutionEngine, SparkDFExecutionEngine)
        ):
            dependencies["column.distinct_values.count"] = MetricConfiguration(
                metric_name="column.distinct_values.count",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )
        return dependencies
