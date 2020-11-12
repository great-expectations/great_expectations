import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricPartialFunctionTypes,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)


class TableMetricProvider(MetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
