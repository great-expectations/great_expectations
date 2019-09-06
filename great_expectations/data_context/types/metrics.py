from six import string_types

from great_expectations.data_context.types import NormalizedDataAssetName
from great_expectations.datasource.types import BatchFingerprint
from great_expectations.types import AllowedKeysDotDict
from great_expectations.profile.metrics_utils import make_dictionary_key

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

# TODO : separate out a MetricIdentifier class, subclassed from DataContextKey,
# so that we can support operations like isinstance(foo, MetricIdentifier)
class Metric(AllowedKeysDotDict):
    """Stores a named metric."""
    _allowed_keys = {
        "metric_name",
        "metric_value"
    }
    _required_keys = {
        "metric_name",
        "metric_value"
    }

# TODO : separate out a NamespaceAwareValidationMetricIdentifier class, subclassed from DataContextKey
class NamespaceAwareValidationMetric(Metric):
    """Captures information from a validation result in a fully namespace aware way suitable to be accessed
    in evaluation parameters, multi-batch validation meta analysis or multi batch validation."""
    _allowed_keys = {
        "data_asset_name",
        "batch_fingerprint",
        "metric_name",
        "metric_kwargs",
        "metric_value"
    }
    _required_keys = {
        "data_asset_name",
        "batch_fingerprint",
        "metric_name",
        "metric_kwargs",
    }
    _key_types = {
        "data_asset_name": NormalizedDataAssetName,
        "batch_fingerprint": BatchFingerprint,
        "metric_name": string_types,
        "metric_kwargs": dict
    }

    @property
    def key(self):
        return ('NamespaceAwareValidationMetric',
                self.data_asset_name,
                self.batch_fingerprint,
                self.metric_name,
                make_dictionary_key(self.metric_kwargs))

    @property
    def multi_batch_key(self):
        return ('NamespaceAwareValidationMetric',
                self.data_asset_name,
                self.metric_name,
                make_dictionary_key(self.metric_kwargs))

# TODO : separate out a NamespaceAwareExpectationDefinedValidationMetricIdentifier class, subclassed from DataContextKey
class NamespaceAwareExpectationDefinedValidationMetric(Metric):
    """Captures information from a validation result in a fully namespace aware way suitable to be accessed
    in evaluation parameters, multi-batch validation meta analysis or multi batch validation."""
    _allowed_keys = {
        "data_asset_name",
        "batch_fingerprint",
        "expectation_type",
        # the path to the key in the result dictionary that holds the metric, encoded as a tuple
        # examples:
        # for {'foo': 1} result_key will be ('foo',),
        # for {'foo': {'bar': 1}} result_key will be ('foo','bar')
        "result_key",
        "metric_kwargs",
        "metric_value"
    }
    _required_keys = {
        "data_asset_name",
        "batch_fingerprint",
        "expectation_type",
        "result_key",
        "metric_kwargs"
    }
    _key_types = {
        "data_asset_name": NormalizedDataAssetName,
        "batch_fingerprint": BatchFingerprint,
        "expectation_type": string_types,
        "result_key": tuple,
        "metric_kwargs": dict
    }

    @property
    def key(self):
        return ('NamespaceAwareExpectationDefinedValidationMetric',
                self.data_asset_name,
                self.batch_fingerprint,
                self.expectation_type,
                self.result_key,
                make_dictionary_key(self.metric_kwargs))

    @property
    def multi_batch_key(self):
        return ('NamespaceAwareExpectationDefinedValidationMetric',
                self.data_asset_name,
                self.expectation_type,
                self.result_key,
                make_dictionary_key(self.metric_kwargs))

# TODO : separate out a MultiBatchNamespaceAwareValidationMetricIdentifier class, subclassed from DataContextKey
class MultiBatchNamespaceAwareValidationMetric(Metric):
    """Holds values of a metric captured from validation results of multiple batches."""

    _allowed_keys = {
        "data_asset_name",
        "metric_name",
        "metric_kwargs",
        "batch_fingerprints",
        "batch_metric_values"
    }
    _required_keys = {
        "data_asset_name",
        "metric_name",
        "metric_kwargs",
        "batch_fingerprints",
        "batch_metric_values"
    }
    _key_types = {
        "data_asset_name": NormalizedDataAssetName,
        "metric_name": string_types,
        "metric_kwargs": dict,
        "batch_fingerprints": list,
        "batch_metric_values": list
    }

    @property
    def key(self):
        return ('MultiBatchNamespaceAwareValidationMetric',
                self.data_asset_name,
                self.metric_name,
                make_dictionary_key(self.metric_kwargs))


# TODO : separate out a MultiBatchNamespaceAwareExpectationDefinedValidationMetricIdentifier class, subclassed from DataContextKey
class MultiBatchNamespaceAwareExpectationDefinedValidationMetric(Metric):
    """Holds values of a metric captured from validation results of multiple batches."""

    _allowed_keys = {
        "data_asset_name",
        # the path to the key in the result dictionary that holds the metric, encoded as a tuple
        # examples:
        # for {'foo': 1} result_key will be ('foo',),
        # for {'foo': {'bar': 1}} result_key will be ('foo','bar')
        "result_key",
        "metric_kwargs",
        "expectation_type",
        "batch_fingerprints",
        "batch_metric_values"
    }
    _required_keys = {
        "data_asset_name",
        "result_key",
        "metric_kwargs",
        "expectation_type",
        "batch_fingerprints",
        "batch_metric_values"
    }
    _key_types = {
        "data_asset_name": NormalizedDataAssetName,
        "result_key": tuple,
        "metric_kwargs": dict,
        "expectation_type": string_types,
        "batch_fingerprints": list,
        "batch_metric_values": list
    }

    @property
    def key(self):
        return ('MultiBatchNamespaceAwareExpectationDefinedValidationMetric',
                self.data_asset_name,
                self.expectation_type,
                self.result_key,
                make_dictionary_key(self.metric_kwargs))
