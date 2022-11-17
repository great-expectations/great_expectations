from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, List, Optional, Sized

import numpy as np
import pandas as pd

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,
)
from great_expectations.types import SerializableDictDot
from great_expectations.types.attributes import Attributes
from great_expectations.validator.computed_metric import MetricValue


def _condition_metric_values(metric_values: MetricValues) -> MetricValues:
    def _detect_illegal_array_type_or_shape(values: MetricValues) -> bool:
        # Python "None" is illegal as candidate for conversion into "numpy.ndarray" type.
        if values is None:
            return True

        # Pandas "DataFrame" and "Series" are illegal as candidates for conversion into "numpy.ndarray" type.
        if isinstance(values, (pd.DataFrame, pd.Series, set)):
            return True

        if isinstance(values, (list, tuple)):
            if len(values) > 0:
                value: MetricValue = values[0]
                # Python "None" is illegal as candidate for conversion into "numpy.ndarray" type.
                if value is None:
                    return True

                # Pandas "DataFrame" and "Series" are illegal as candidates for conversion into "numpy.ndarray" type.
                if isinstance(value, (pd.DataFrame, pd.Series, set)):
                    return True

                # Components of different lengths cannot be packaged into "numpy.ndarray" type (due to undefined shape).
                if all(isinstance(value, (list, tuple)) for value in values):
                    values_iterator: Iterator = iter(values)
                    first_value_length: int = len(next(values_iterator))
                    current_value: Sized[Any]
                    if not all(
                        len(current_value) == first_value_length
                        for current_value in values_iterator
                    ):
                        return True

                # Recursively evaluate each element of properly shaped iterable (list or tuple).
                for value in values:
                    if _detect_illegal_array_type_or_shape(values=value):
                        return True

        return False

    if _detect_illegal_array_type_or_shape(values=metric_values):
        value: MetricValue
        if (
            metric_values is not None
            and isinstance(metric_values, (list, tuple))
            and all(value is None for value in metric_values)
        ):
            return np.asarray(metric_values)

        return metric_values
    else:
        return np.asarray(metric_values)


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
    def get_conditioned_attributed_metric_values_from_attributed_metric_values(
        attributed_metric_values: Dict[str, MetricValues]
    ) -> Dict[str, MetricValues]:
        """
        Converts "attributed_metric_values" to Numpy array for each "batch_id" key (recursively, wherever possible).
        """
        if attributed_metric_values is None:
            return {}

        batch_id: str
        metric_values: MetricValues
        return {
            batch_id: _condition_metric_values(metric_values=metric_values)
            for batch_id, metric_values in attributed_metric_values.items()
        }

    @staticmethod
    def get_conditioned_metric_values_from_attributed_metric_values(
        attributed_metric_values: Dict[str, MetricValue]
    ) -> Optional[MetricValues]:
        """
        Converts all "attributed_metric_values" as list (together) to Numpy array (recursively, wherever possible).
        """
        if attributed_metric_values is None:
            return None

        metric_values_all_batches: MetricValues = list(
            attributed_metric_values.values()
        )
        return _condition_metric_values(metric_values=metric_values_all_batches)

    def add_resolved_metric(self, batch_id: str, value: MetricValue) -> None:
        """
        Adds passed resolved metric "value" to list for corresponding "batch_id" key.
        """
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
    def conditioned_attributed_metric_values(self) -> Dict[str, MetricValues]:
        return AttributedResolvedMetrics.get_conditioned_attributed_metric_values_from_attributed_metric_values(
            attributed_metric_values=self.attributed_metric_values
        )

    @property
    def conditioned_metric_values(self) -> MetricValues:
        return AttributedResolvedMetrics.get_conditioned_metric_values_from_attributed_metric_values(
            attributed_metric_values=self.attributed_metric_values
        )

    def to_dict(self) -> dict:
        """
        Returns dictionary equivalent of this object.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Returns JSON dictionary equivalent of this object.
        """
        return convert_to_json_serializable(data=self.to_dict())
