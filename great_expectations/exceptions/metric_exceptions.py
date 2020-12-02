"""Exceptions related to metrics"""
from great_expectations.exceptions import GreatExpectationsError


class MetricError(GreatExpectationsError):
    pass


class MetricProviderError(MetricError):
    pass
