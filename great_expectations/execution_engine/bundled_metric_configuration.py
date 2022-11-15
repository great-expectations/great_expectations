from dataclasses import asdict, dataclass
from typing import Any

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import DictDot
from great_expectations.validator.metric_configuration import MetricConfiguration


@dataclass(frozen=True)
class BundledMetricConfiguration(DictDot):
    """
    BundledMetricConfiguration is a "dataclass" object, which holds components required for bundling metric computation.
    """

    metric_configuration: MetricConfiguration
    metric_fn: Any
    compute_domain_kwargs: dict
    accessor_domain_kwargs: dict
    metric_provider_kwargs: dict

    def to_dict(self) -> dict:
        """Returns: this BundledMetricConfiguration as a dictionary"""
        return asdict(self)

    def to_json_dict(self) -> dict:
        """Returns: this BundledMetricConfiguration as a JSON dictionary"""
        return convert_to_json_serializable(data=self.to_dict())
