import logging
from typing import Tuple

from great_expectations.core._docs_decorators import public_api
from great_expectations.expectations.metrics.metric_provider import MetricProvider

logger = logging.getLogger(__name__)


@public_api
class TableMetricProvider(MetricProvider):
    """Base class for all Table Metrics, which define metrics to be calculated across a complete table.

    An example of this is `table.column.unique`,
    which verifies that a table contains unique, non-duplicate columns.

    Args:
        metric_name (str): A name identifying the metric. Metric Name must be globally unique in
            a great_expectations installation.
        domain_keys (tuple): A tuple of the keys used to determine the domain of the metric.
        value_keys (tuple): A tuple of the keys used to determine the value of the metric.

    In some cases, subclasses of MetricProvider, such as TableMetricProvider, will already
    have correct values that may simply be inherited by Metric classes.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations
    """

    domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
