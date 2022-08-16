import logging

from great_expectations.expectations.metrics.metric_provider import MetricProvider

logger = logging.getLogger(__name__)


class QueryMetricProvider(MetricProvider):
    """Base class for all Query Metrics.
       Query Metric classes inheriting from QueryMetricProvider *must* have the following attributes set:
        1. `metric_name`: the name to use. Metric Name must be globally unique in
           a great_expectations installation.
        1. `domain_keys`: a tuple of the *keys* used to determine the domain of the
           metric
        2. `value_keys`: a tuple of the *keys* used to determine the value of
           the metric.

    In some cases, subclasses of MetricProvider, such as QueryMetricProvider, will already
    have correct values that may simply be inherited by Metric classes.
    """

    domain_keys = ("batch_id", "row_condition", "condition_parser")
