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
    ColumnMetricProvider,
    column_aggregate_metric,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnPartition(ColumnMetricProvider):
    metric_name = "column.aggregate.max"
    value_keys = ("bins", "n_bins", "allow_relative_error")
    default_kwarg_values = {
        "bins": "uniform",
        "n_bins": 10,
        "allow_relative_error": False,
    }

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, bins, n_bins, allow_relative_error, _metrics, **kwargs):
        return _get_column_partition_using_metrics(bins, n_bins, _metrics)

    @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls, column, bins, n_bins, allow_relative_error, _metrics, **kwargs
    ):
        return _get_column_partition_using_metrics(bins, n_bins, _metrics)

    @column_aggregate_metric(engine=SparkDFExecutionEngine)
    def _spark(cls, column, bins, n_bins, allow_relative_error, _metrics, **kwargs):
        return _get_column_partition_using_metrics(bins, n_bins, _metrics)

    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        bins = metric.metric_value_kwargs["bins"]
        n_bins = metric.metric_value_kwargs["n_bins"]
        allow_relative_error = metric.metric_value_kwargs["allow_relative_error"]

        if bins == "uniform":
            return {
                "column.aggregate.min": MetricConfiguration(
                    "column.aggregate.min", metric.metric_domain_kwargs
                ),
                "column.aggregate.max": MetricConfiguration(
                    "column.aggregate.max", metric.metric_domain_kwargs
                ),
            }
        elif bins in ["ntile", "quantile", "percentile"]:
            return {
                "column.quantile_values": MetricConfiguration(
                    "column.aggregate.quantile.values",
                    metric.metric_domain_kwargs,
                    {
                        "quantiles": tuple(
                            np.linspace(start=0, stop=1, num=n_bins + 1)
                        ),
                        "allow_relative_error": allow_relative_error,
                    },
                )
            }
        elif bins == "auto":
            return {
                "column_values.nonnull.unexpected_count": MetricConfiguration(
                    "column_values.nonnull.unexpected_count",
                    metric.metric_domain_kwargs,
                ),
                "column.quantile_values": MetricConfiguration(
                    "column.aggregate.quantile.values",
                    metric.metric_domain_kwargs,
                    {
                        "quantiles": (0, 0.25, 0.75, 1.0),
                        "allow_relative_error": allow_relative_error,
                    },
                ),
            }
        else:
            raise ValueError("Invalid parameter for bins argument")


def _get_column_partition_using_metrics(bins, n_bins, _metrics):
    if bins == "uniform":
        min_ = _metrics["column.aggregate.min"]
        max_ = _metrics["column.aggregate.max"]
        # PRECISION NOTE: some implementations of quantiles could produce
        # varying levels of precision (e.g. a NUMERIC column producing
        # Decimal from a SQLAlchemy source, so we cast to float for numpy)
        bins = np.linspace(start=float(min_), stop=float(max_), num=n_bins + 1)
    elif bins in ["ntile", "quantile", "percentile"]:
        bins = _metrics["column.quantile_values"]
    elif bins == "auto":
        # Use the method from numpy histogram_bin_edges
        nonnull_count = _metrics["column_values.nonnull.unexpected_count"]
        sturges = np.log2(nonnull_count + 1)
        min_, _25, _75, max_ = _metrics["column.quantile_values"]
        iqr = _75 - _25
        if iqr < 1e-10:  # Consider IQR 0 and do not use variance-based estimator
            n_bins = sturges
        else:
            fd = (2 * float(iqr)) / (nonnull_count ** (1 / 3))
            n_bins = max(int(np.ceil(sturges)), int(np.ceil(float(max_ - min_) / fd)))
        bins = np.linspace(start=float(min_), stop=float(max_), num=n_bins + 1)
    else:
        raise ValueError("Invalid parameter for bins argument")
    return bins
