from six import string_types

from great_expectations.data_context.types import NormalizedDataAssetName
from great_expectations.types import AllowedKeysDotDict

from collections import OrderedDict
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

# TODO: data_asset_name_delimiter is not taken from a namespace yet
DATA_ASSET_NAME_DELIMITER = "/"


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
        "batch_fingerprint": string_types,
        "metric_name": string_types,
        "metric_kwargs": dict
    }

    @property
    def urn(self):
        urn = "urn:great_expectations:" + DATA_ASSET_NAME_DELIMITER.join(self.data_asset_name) + ":" + \
            self.batch_fingerprint + ":" + self.metric_name + ":" + \
            urlencode(OrderedDict(sorted(self.metric_kwargs.items())))
        if hasattr(self, "metric_value") and self.metric_value is not None:
            urn += ":" + str(self.metric_value)
        return urn
