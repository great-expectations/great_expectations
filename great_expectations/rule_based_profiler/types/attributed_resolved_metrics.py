from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.types import MetricValue, MetricValues
from great_expectations.types import SerializableDictDot
from great_expectations.types.attributes import Attributes


@dataclass
class AttributedResolvedMetrics(SerializableDictDot):
    """
    This class facilitates computing multiple metrics as one operation.

    In order to gather results pertaining to diverse MetricConfiguration directives, computed metrics are augmented
    with uniquely identifiable attribution object so that receivers can filter them from overall resolved metrics.
    """

    batch_ids: Optional[List[str]] = None
    metric_attributes: Optional[Attributes] = None
    metric_values_by_batch_id: Optional[Dict[str, MetricValue]] = None

    @staticmethod
    def get_metric_values_from_attributed_metric_values(
        attributed_metric_values: Dict[str, MetricValue]
    ) -> MetricValues:
        if attributed_metric_values is None:
            return None

        values: MetricValues = list(attributed_metric_values.values())[0]
        if values is not None and isinstance(values, (pd.DataFrame, pd.Series)):
            return list(attributed_metric_values.values())

        return np.array(list(attributed_metric_values.values()))

    def add_resolved_metric(self, batch_id: str, value: MetricValue) -> None:
        if self.metric_values_by_batch_id is None:
            self.metric_values_by_batch_id = {}

        self.metric_values_by_batch_id[batch_id] = value

    @property
    def id(self) -> str:
        return self.metric_attributes.to_id()

    @property
    def attributed_metric_values(self) -> Optional[Dict[str, MetricValue]]:
        if self.metric_values_by_batch_id is None:
            return None

        batch_id: str
        return {
            batch_id: self.metric_values_by_batch_id.get(batch_id)
            for batch_id in self.batch_ids
            if batch_id in self.metric_values_by_batch_id
        }

    @property
    def metric_values(self) -> MetricValues:
        return (
            AttributedResolvedMetrics.get_metric_values_from_attributed_metric_values(
                attributed_metric_values=self.attributed_metric_values
            )
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_dict(self) -> dict:
        return convert_to_json_serializable(data=self.to_dict())
