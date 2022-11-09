from __future__ import annotations

import json
from typing import Dict, Optional, Tuple

from great_expectations.core.id_dict import IDDict
from great_expectations.core.util import convert_to_json_serializable


class MetricConfiguration:
    def __init__(
        self,
        metric_name: str,
        metric_domain_kwargs: dict,
        metric_value_kwargs: Optional[dict] = None,
    ) -> None:
        self._metric_name: str = metric_name

        if not isinstance(metric_domain_kwargs, IDDict):
            metric_domain_kwargs = IDDict(metric_domain_kwargs)

        self._metric_domain_kwargs: IDDict = metric_domain_kwargs

        if not isinstance(metric_value_kwargs, IDDict):
            if metric_value_kwargs is None:
                metric_value_kwargs = {}

            metric_value_kwargs = IDDict(metric_value_kwargs)

        self._metric_value_kwargs: IDDict = metric_value_kwargs

        self.metric_dependencies: Dict[str, MetricConfiguration] = {}

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return self.__repr__()

    @property
    def metric_name(self) -> str:
        return self._metric_name

    @property
    def metric_domain_kwargs(self) -> IDDict:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> IDDict:
        return self._metric_value_kwargs

    @property
    def metric_domain_kwargs_id(self) -> str:
        return self.metric_domain_kwargs.to_id()

    @property
    def metric_value_kwargs_id(self) -> str:
        return self.metric_value_kwargs.to_id()

    @property
    def id(self) -> Tuple[str, str, str]:
        return (
            self.metric_name,
            self.metric_domain_kwargs_id,
            self.metric_value_kwargs_id,
        )

    def to_json_dict(self) -> dict:
        json_dict: dict = convert_to_json_serializable(
            data={
                "metric_name": self.metric_name,
                "metric_domain_kwargs": self.metric_domain_kwargs,
                "metric_domain_kwargs_id": self.metric_domain_kwargs_id,
                "metric_value_kwargs": self.metric_value_kwargs,
                "metric_value_kwargs_id": self.metric_value_kwargs_id,
                "id": self.id,
            }
        )
        return json_dict
