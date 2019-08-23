from six import string_types

from great_expectations.data_context.types import NormalizedDataAssetName
from great_expectations.types import AllowedKeysDotDict

from collections import OrderedDict
try:
    from urllib.parse import urlencode, parse_qsl
except ImportError:
    from urllib import urlencode, parse_qsl

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

    @classmethod
    def from_urn(cls, urn):
        # TODO: Triage importance of dealing with case where : is in a component of the URN (e.g. batch_fingerprint)
        urn_parts = urn.split(":")

        if not 5 < len(urn_parts) < 8:
            raise ValueError("Unrecognized URN format for NamespaceAwareValidationMetric. There must be 6 or 7 "
                             "components.")

        if not urn_parts[1] == "great_expectations":
            raise ValueError("Unrecognized URN format for NamespaceAwareValidationMetric. URN must begin with "
                             "urn:great_expectations and no urn component may use ':'")

        # Type coercion can only happen on instantiation
        if len(urn_parts) == 7:
            metric = cls(
                data_asset_name=NormalizedDataAssetName(
                    *urn_parts[2].split(DATA_ASSET_NAME_DELIMITER)
                ),
                batch_fingerprint=urn_parts[3],
                metric_name=urn_parts[4],
                metric_kwargs=dict(parse_qsl(urn_parts[5])),
                metric_value=urn_parts[6],
                coerce_types=True
            )
        else:
            metric = cls(
                data_asset_name=NormalizedDataAssetName(
                    *urn_parts[2].split(DATA_ASSET_NAME_DELIMITER)
                ),
                batch_fingerprint=urn_parts[3],
                metric_name=urn_parts[4],
                metric_kwargs=dict(parse_qsl(urn_parts[5])),
                coerce_types=True
            )
        return metric

    @property
    def urn(self):
        urn = "urn:great_expectations:" + DATA_ASSET_NAME_DELIMITER.join(self.data_asset_name) + ":" + \
            self.batch_fingerprint + ":" + self.metric_name + ":" + \
            urlencode(OrderedDict(sorted(self.metric_kwargs.items())))
        if hasattr(self, "metric_value") and self.metric_value is not None:
            urn += ":" + str(self.metric_value)
        return urn
