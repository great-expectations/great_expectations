import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple, Type

import sqlalchemy as sa

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric import Metric, metric

logger = logging.getLogger(__name__)

try:
    import pyspark.sql.functions as F
    import pyspark.sql.types as sparktypes
    from pyspark.ml.feature import Bucketizer
    from pyspark.sql import DataFrame, SQLContext, Window
    from pyspark.sql.functions import (
        array,
        col,
        count,
        countDistinct,
        datediff,
        desc,
        expr,
        isnan,
        lag,
    )
    from pyspark.sql.functions import length as length_
    from pyspark.sql.functions import (
        lit,
        monotonically_increasing_id,
        stddev_samp,
        udf,
        when,
        year,
    )

except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


def table_metric(engine: Type[ExecutionEngine], **kwargs):
    """Return the column aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=PandasExecutionEngine, bundle_metric=True)
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "PandasExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                table = execution_engine.get_domain_dataframe(
                    domain_kwargs=metric_domain_kwargs,
                )
                return metric_fn(cls, table, **metric_value_kwargs, _metrics=metrics)

            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=SqlAlchemyExecutionEngine, bundle_metric=True)
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SqlAlchemyExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                dialect = execution_engine.dialect
                table = execution_engine._get_selectable(metric_domain_kwargs)
                expected_condition = metric_fn(
                    cls,
                    table ** metric_value_kwargs,
                    _dialect=dialect,
                    _metrics=metrics,
                )
                return expected_condition, table

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=SparkDFExecutionEngine, bundle_metric=True)
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SparkDFExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                table = execution_engine.get_domain_dataframe(
                    domain_kwargs=metric_domain_kwargs
                )
                # eval_col = execution_engine._get_eval_column_name(
                #     metric_domain_kwargs.get("column")
                # )
                expected_condition = metric_fn(
                    cls,
                    table=table,
                    metric_domain_kwargs=metric_domain_kwargs,
                    metric_value_kwargs=metric_value_kwargs,
                    metrics=metrics,
                    **kwargs,
                )
                return expected_condition

            return inner_func

        return wrapper

    else:
        raise ValueError("Unsupported engine for column_aggregate_metric")


class TableMetric(Metric):
    domain_keys = (
        "batch_id",
        "table",
    )
    bundle_metric = True
