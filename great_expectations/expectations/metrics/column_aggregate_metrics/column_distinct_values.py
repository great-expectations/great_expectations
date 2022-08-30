from typing import Any, Dict, Optional, Set

import numpy as np
import pandas as pd

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
        **kwargs,
    ) -> Set[Any]:
        (selectable, _, accessor_domain_kwargs,) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column_name: str = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        sqlalchemy_engine = execution_engine.engine

        distinct_values = sqlalchemy_engine.execute(
            sa.select([column])
            .where(column.is_not(None))
            .distinct()
            .select_from(selectable)
        ).fetchall()
        # Vectorized operation is not faster here due to overhead of converting to and from numpy array
        return {row[0] for row in distinct_values}

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        **kwargs,
    ) -> Set[Any]:
        (df, _, accessor_domain_kwargs,) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column_name: str = accessor_domain_kwargs["column"]
        distinct_values = (
            df.distinct()
            .where(F.col(column_name).isNotNull())
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        pandas_df = pd.DataFrame(
            distinct_values, columns=["distinct_values"]
        ).convert_dtypes()
        return set(pandas_df["distinct_values"])


class ColumnDistinctValuesCount(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        metrics: Dict[str, Any],
        **kwargs,
    ) -> int:
        column_distinct_values = metrics["column.distinct_values"]
        return len(column_distinct_values)

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        **kwargs,
    ):
        return sa.func.count(sa.distinct(column))

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        **kwargs,
    ):
        return F.countDistinct(column)

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
        if (
            metric.metric_name == "column.distinct_values.count"
            and execution_engine == PandasExecutionEngine
        ):
            dependencies["column.distinct_values.count"] = MetricConfiguration(
                metric_name="column.distinct_values",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )
        return dependencies


class ColumnDistinctValuesCountUnderThreshold(ColumnAggregateMetricProvider):
    metric_name = "column.distinct_values.count.under_threshold"
    condition_keys = ("threshold",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        **kwargs,
    ) -> bool:
        column_distinct_values_count = metrics["column.distinct_values.count"]
        threshold = metric_value_kwargs["threshold"]
        return column_distinct_values_count < threshold

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        **kwargs,
    ) -> bool:
        observed_distinct_values_count = metrics["column.distinct_values.count"]
        threshold = metric_value_kwargs["threshold"]
        return observed_distinct_values_count < threshold

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        **kwargs,
    ) -> bool:
        observed_distinct_values_count = metrics["column.distinct_values.count"]
        threshold = metric_value_kwargs["threshold"]
        return observed_distinct_values_count < threshold

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
