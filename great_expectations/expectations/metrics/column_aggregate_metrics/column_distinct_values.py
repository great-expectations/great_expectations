from typing import Any, Dict, Optional, Tuple

import pandas as pd

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
    ColumnMetricProvider,
)
from great_expectations.expectations.metrics.column_aggregate_metric import F as F
from great_expectations.expectations.metrics.column_aggregate_metric import (
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.metric_provider import metric
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnDistinctValues(ColumnMetricProvider):
    metric_name = "column.distinct_values"

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return set(column.unique())

    @metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        observed_value_counts = metrics["column.value_counts"]
        return set(observed_value_counts.index)

    @metric(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        observed_value_counts = metrics["column.value_counts"]
        return set(observed_value_counts.index)

    @classmethod
    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[Dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration,
        specifying the metric types and their respective domains"""
        if isinstance(
            ExecutionEngine, (SqlAlchemyExecutionEngine, SparkDFExecutionEngine)
        ):
            return {
                "column.value_counts": MetricConfiguration(
                    metric_name="column.value_counts",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs={"sort": "value", "collate": None},
                )
            }

        return dict()


class ColumnDistinctValueCounts(ColumnMetricProvider):
    metric_name = "column.value_counts"
    value_keys = ("sort", "collate")

    default_kwarg_values = {"sort": "value", "collate": None}

    @metric(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        sort = metric_value_kwargs["sort"]
        collate = metric_value_kwargs["collate"]

        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        if collate is not None:
            raise ValueError("collate parameter is not supported in PandasDataset")

        df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )
        column = accessor_domain_kwargs["column"]

        counts = df.value_counts()
        # Convert to flat index, since we are looking only at a single column, but
        # value_counts returns a multiindex
        counts.index = [val[0] for val in counts.index]
        if sort == "value":
            try:
                counts.sort_index(inplace=True)
            except TypeError:
                # Having values of multiple types in a object dtype column (e.g., strings and floats)
                # raises a TypeError when the sorting method performs comparisons.
                if df[column].dtype == object:
                    counts.index = counts.index.astype(str)
                    counts.sort_index(inplace=True)
        elif sort == "counts":
            counts.sort_values(inplace=True)
        counts.name = "count"
        counts.index.name = "value"
        return counts

    @metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        sort = metric_value_kwargs["sort"]
        collate = metric_value_kwargs["collate"]

        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        if collate is not None:
            raise ValueError("collate parameter is not supported in PandasDataset")

        selectable, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )
        column = accessor_domain_kwargs["column"]

        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")

        query = (
            sa.select(
                [
                    sa.column(column).label("value"),
                    sa.func.count(sa.column(column)).label("count"),
                ]
            )
            .where(sa.column(column) != None)
            .group_by(sa.column(column))
        )
        if sort == "value":
            # NOTE: depending on the way the underlying database collates columns,
            # ordering can vary. postgresql collate "C" matches default sort
            # for python and most other systems, but is not universally supported,
            # so we use the default sort for the system, unless specifically overridden
            if collate is not None:
                query = query.order_by(sa.column(column).collate(collate))
            else:
                query = query.order_by(sa.column(column))
        elif sort == "count":
            query = query.order_by(sa.column("count").desc())
        results = execution_engine.engine.execute(
            query.select_from(selectable)
        ).fetchall()
        series = pd.Series(
            [row[1] for row in results],
            index=pd.Index(data=[row[0] for row in results], name="value"),
            name="count",
        )
        return series

    @metric(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        sort = metric_value_kwargs["sort"]
        collate = metric_value_kwargs["collate"]

        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        if collate is not None:
            raise ValueError("collate parameter is not supported in SparkDFDataset")

        df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )
        column = accessor_domain_kwargs["column"]

        value_counts = (
            df.select(column).where(F.col(column).isNotNull()).groupBy(column).count()
        )
        if sort == "value":
            value_counts = value_counts.orderBy(column)
        elif sort == "count":
            value_counts = value_counts.orderBy(F.desc("count"))
        value_counts = value_counts.collect()
        series = pd.Series(
            [row["count"] for row in value_counts],
            index=pd.Index(data=[row[column] for row in value_counts], name="value"),
            name="count",
        )
        return series
