from six import PY3

from great_expectations.core import DataContextKey
from great_expectations.profile.metrics_utils import kwargs_to_tuple, tuple_to_hash

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode


class Metric(object):
    def __init__(self, metric_name, metric_value):
        self._metric_name = metric_name
        self._metric_value = metric_value

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_value(self):
        return self._metric_value

    def to_tuple(self):
        return self.metric_name, self.metric_value

class MetricIdentifier(DataContextKey):
    # def __init__(self, data_asset_name, batch_kwargs, batch_id, metric_name, metric_kwargs, metric_value):
    def __init__(self, data_asset_name, batch_fingerprint, metric_name, metric_kwargs, metric_value):
        self._data_asset_name = data_asset_name
        self._batch_fingerprint = batch_fingerprint
        self._metric_name = metric_name,
        self._metric_kwargs = metric_kwargs
        self._metric_value = metric_value

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def batch_fingerprint(self):
        return self._batch_fingerprint

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_kwargs(self):
        return self._metric_kwargs

    def to_tuple(self):
        return (self.data_asset_name, self.batch_fingerprint, self.metric_name,
                kwargs_to_tuple(self.metric_kwargs))


class ExpectationDefinedMetricIdentifier(DataContextKey):
    def __init__(self, run_id, data_asset_name, expectation_suite_name, expectation_type, metric_name, metric_kwargs):
        self._run_id = run_id
        self._data_asset_name = data_asset_name
        self._expectation_suite_name = expectation_suite_name
        self._expectation_type = expectation_type
        self._metric_name = metric_name
        self._metric_kwargs = metric_kwargs

    @property
    def run_id(self):
        return self._run_id

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def expectation_suite_name(self):
        return self._expectation_suite_name

    @property
    def expectation_type(self):
        return self._expectation_type

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_kwargs(self):
        return self._metric_kwargs

    def to_tuple(self):
        # if PY3:
        #     return (self.run_id, *self.data_asset_name.to_tuple(), self.expectation_suite_name, self.expectation_type,
        #             self.metric_name, kwargs_to_tuple(self.metric_kwargs))
        # else:
        expectation_defined_metric_tuple_list = [self.run_id] + list(self.data_asset_name.to_tuple()) + [
            self.expectation_suite_name, self.expectation_type, self.metric_name,
            kwargs_to_tuple(self.metric_kwargs)]
        return tuple(expectation_defined_metric_tuple_list)

    def to_string_tuple(self):
        # if PY3:
        #     return (self.run_id, *self.data_asset_name.to_tuple(), self.expectation_suite_name, self.expectation_type,
        #             self.metric_name, tuple_to_hash(kwargs_to_tuple(self.metric_kwargs)))
        # else:
        expectation_defined_metric_tuple_list = [self.run_id] + list(self.data_asset_name.to_tuple()) + [
            self.expectation_suite_name, self.expectation_type, self.metric_name,
            tuple_to_hash(kwargs_to_tuple(self.metric_kwargs))]
        return tuple(expectation_defined_metric_tuple_list)

    def to_urn(self):
        urn = "urn:great_expectations:validations:" + \
            self.data_asset_name.to_path() + ":" + \
            self.expectation_suite_name + ":" + \
            "expectations" + ":" + \
            self.expectation_type + ":"
        if "column" in self.metric_kwargs:
            urn += "columns:" + self.metric_kwargs["column"] + ":"
        urn += self.metric_name
        return urn


class ExpectationDefinedMetric(object):
    def __init__(self, metric_identifier, metric_value):
        self._metric_identifier = metric_identifier
        self._metric_value = metric_value

    @property
    def run_id(self):
        return self._run_id

    @property
    def metric_identifier(self):
        return self._metric_identifier

    @property
    def metric_value(self):
        return self._metric_value
