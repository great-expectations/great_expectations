import logging
from typing import Tuple

from great_expectations.expectations.metrics.metric_provider import MetricProvider

logger = logging.getLogger(__name__)


class TableMetricProvider(MetricProvider):
    domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
