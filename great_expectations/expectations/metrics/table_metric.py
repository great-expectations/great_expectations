import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple, Type

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value_fn,
)

logger = logging.getLogger(__name__)

class TableMetricProvider(MetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    metric_fn_type = "aggregate_fn"
