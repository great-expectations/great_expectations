"""Exceptions related to metrics"""
from collections import Iterable

from great_expectations.exceptions import GreatExpectationsError


class MetricError(GreatExpectationsError):
    pass


class MetricProviderError(MetricError):
    pass


class MetricResolutionError(MetricError):
    def __init__(self, message, failed_metrics):
        super().__init__(message)
        if not isinstance(failed_metrics, Iterable):
            failed_metrics = (failed_metrics,)
        self.failed_metrics = failed_metrics
