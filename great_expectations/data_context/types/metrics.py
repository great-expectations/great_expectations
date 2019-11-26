from great_expectations.core import DataContextKey
from great_expectations.profile.metrics_utils import kwargs_to_tuple

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

#
#
# # TODO : separate out a MetricIdentifier class, subclassed from DataContextKey,
# # so that we can support operations like isinstance(foo, MetricIdentifier)
# class Metric(AllowedKeysDotDict):
#     """Stores a named metric."""
#     _allowed_keys = {
#         "metric_name",
#         "metric_value"
#     }
#     _required_keys = {
#         "metric_name",
#         "metric_value"
#     }


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
        return (self.data_asset_name, self.expectation_suite_name, self.expectation_type,
                self.metric_name, kwargs_to_tuple(self.metric_kwargs))

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

#
#
# class ValidationMetric(object):
#     def __init__(self, metric_identifier, metric_value):
#         super(ValidationMetric, self).__init__()
#         if isinstance(metric_identifier, MetricIdentifier):
#            self._metric_identifier = metric_identifier
#         elif isinstance(metric_identifier, (list, tuple)):
#             self._metric_identifier = MetricIdentifier(*metric_identifier)
#         else:
#             self._metric_identifier = MetricIdentifier(**metric_identifier)
#         self._metric_value = metric_value
#
#     @property
#     def metric_identifier(self):
#         return self._metric_identifier
#
#     @property
#     def metric_value(self):
#         return self._metric_value
#
#     @property
#     def key(self):
#         return ('ValidationMetric',
#                 self.metric_identifier.data_asset_name,
#                 self.metric_identifier.batch_fingerprint,
#                 self.metric_identifier.metric_name,
#                 make_dictionary_key(self.metric_identifier.metric_kwargs))
#
#     @property
#     def multi_batch_key(self):
#         return ('ValidationMetric',
#                 self.metric_identifier.data_asset_name,
#                 self.metric_identifier.metric_name,
#                 make_dictionary_key(self.metric_identifier.metric_kwargs))

#
# # TODO : separate out a NamespaceAwareValidationMetricIdentifier class, subclassed from DataContextKey
# class NamespaceAwareValidationMetric(Metric):
#     pass
#     """Captures information from a validation result in a fully namespace aware way suitable to be accessed
#     in evaluation parameters, multi-batch validation meta analysis or multi batch validation."""
#     _allowed_keys = {
#         "data_asset_name",
#         "batch_fingerprint",
#         "metric_name",
#         "metric_kwargs",
#         "metric_value"
#     }
#     _required_keys = {
#         "data_asset_name",
#         "batch_fingerprint",
#         "metric_name",
#         "metric_kwargs",
#     }
#     _key_types = {
#         "data_asset_name": NormalizedDataAssetName,
#         "batch_fingerprint": BatchFingerprint,
#         "metric_name": string_types,
#         "metric_kwargs": dict
#     }
#
#     @property
#     def key(self):
#         return ('NamespaceAwareValidationMetric',
#                 self.data_asset_name,
#                 self.batch_fingerprint,
#                 self.metric_name,
#                 make_dictionary_key(self.metric_kwargs))

    # @property
    # def multi_batch_key(self):
    #     return ('NamespaceAwareValidationMetric',
    #             self.data_asset_name,
    #             self.metric_name,
    #             make_dictionary_key(self.metric_kwargs))
#
# # TODO : separate out a NamespaceAwareExpectationDefinedValidationMetricIdentifier class, subclassed from DataContextKey
# class NamespaceAwareExpectationDefinedValidationMetric(Metric):
#     """Captures information from a validation result in a fully namespace aware way suitable to be accessed
#     in evaluation parameters, multi-batch validation meta analysis or multi batch validation."""
#     _allowed_keys = {
#         "data_asset_name",
#         "batch_fingerprint",
#         "expectation_type",
#         # the path to the key in the result dictionary that holds the metric, encoded as a tuple
#         # examples:
#         # for {'foo': 1} result_key will be ('foo',),
#         # for {'foo': {'bar': 1}} result_key will be ('foo','bar')
#         "result_key",
#         "metric_kwargs",
#         "metric_value"
#     }
#     _required_keys = {
#         "data_asset_name",
#         "batch_fingerprint",
#         "expectation_type",
#         "result_key",
#         "metric_kwargs"
#     }
#     _key_types = {
#         "data_asset_name": NormalizedDataAssetName,
#         "batch_fingerprint": BatchFingerprint,
#         "expectation_type": string_types,
#         "result_key": tuple,
#         "metric_kwargs": dict
#     }
#
#     @property
#     def key(self):
#         return ('NamespaceAwareExpectationDefinedValidationMetric',
#                 self.data_asset_name,
#                 self.batch_fingerprint,
#                 self.expectation_type,
#                 self.result_key,
#                 make_dictionary_key(self.metric_kwargs))
#
#     @property
#     def multi_batch_key(self):
#         return ('NamespaceAwareExpectationDefinedValidationMetric',
#                 self.data_asset_name,
#                 self.expectation_type,
#                 self.result_key,
#                 make_dictionary_key(self.metric_kwargs))

# # TODO : separate out a MultiBatchNamespaceAwareValidationMetricIdentifier class, subclassed from DataContextKey
# class MultiBatchNamespaceAwareValidationMetric(Metric):
#     """Holds values of a metric captured from validation results of multiple batches."""
#
#     _allowed_keys = {
#         "data_asset_name",
#         "metric_name",
#         "metric_kwargs",
#         "batch_fingerprints",
#         "batch_metric_values"
#     }
#     _required_keys = {
#         "data_asset_name",
#         "metric_name",
#         "metric_kwargs",
#         "batch_fingerprints",
#         "batch_metric_values"
#     }
#     _key_types = {
#         "data_asset_name": NormalizedDataAssetName,
#         "metric_name": string_types,
#         "metric_kwargs": dict,
#         "batch_fingerprints": list,
#         "batch_metric_values": list
#     }
#
#     @property
#     def key(self):
#         return ('MultiBatchNamespaceAwareValidationMetric',
#                 self.data_asset_name,
#                 self.metric_name,
#                 make_dictionary_key(self.metric_kwargs))
#
#
# # TODO : separate out a MultiBatchNamespaceAwareExpectationDefinedValidationMetricIdentifier class, subclassed from DataContextKey
# class MultiBatchNamespaceAwareExpectationDefinedValidationMetric(Metric):
#     """Holds values of a metric captured from validation results of multiple batches."""
#
#     _allowed_keys = {
#         "data_asset_name",
#         # the path to the key in the result dictionary that holds the metric, encoded as a tuple
#         # examples:
#         # for {'foo': 1} result_key will be ('foo',),
#         # for {'foo': {'bar': 1}} result_key will be ('foo','bar')
#         "result_key",
#         "metric_kwargs",
#         "expectation_type",
#         "batch_fingerprints",
#         "batch_metric_values"
#     }
#     _required_keys = {
#         "data_asset_name",
#         "result_key",
#         "metric_kwargs",
#         "expectation_type",
#         "batch_fingerprints",
#         "batch_metric_values"
#     }
#     _key_types = {
#         "data_asset_name": NormalizedDataAssetName,
#         "result_key": tuple,
#         "metric_kwargs": dict,
#         "expectation_type": string_types,
#         "batch_fingerprints": list,
#         "batch_metric_values": list
#     }
#
#     @property
#     def key(self):
#         return ('MultiBatchNamespaceAwareExpectationDefinedValidationMetric',
#                 self.data_asset_name,
#                 self.expectation_type,
#                 self.result_key,
#                 make_dictionary_key(self.metric_kwargs))
