import logging
from dataclasses import asdict, dataclass
from typing import Dict, Iterator, List, Optional, Sized

import numpy as np
import pandas as pd

from great_expectations.compatibility import pyspark, sqlalchemy
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.rule_based_profiler.metric_computation_result import (
    MetricValues,
)
from great_expectations.types import SerializableDictDot
from great_expectations.types.attributes import Attributes
from great_expectations.util import deep_filter_properties_iterable
from great_expectations.validator.computed_metric import MetricValue

logger = logging.getLogger(__name__)


def _condition_metric_values(metric_values: MetricValues) -> MetricValues:
    def _detect_illegal_array_type_or_shape(values: MetricValues) -> bool:
        # Pandas "DataFrame" and "Series" are illegal as candidates for conversion into "numpy.ndarray" type.
        if isinstance(
            values,
            deep_filter_properties_iterable(
                properties=(
                    pd.DataFrame,
                    pd.Series,
                    sqlalchemy.Row if sqlalchemy.Row else None,
                    pyspark.Row if pyspark.Row else None,  # type: ignore[truthy-function]
                    set,
                )
            ),
        ):
            return True

        value: MetricValue
        if isinstance(values, (list, tuple)):
            if values is not None and len(values) > 0:
                values = deep_filter_properties_iterable(properties=values)
                if values:
                    values_iterator: Iterator
                    # Components of different lengths cannot be packaged into "numpy.ndarray" type (due to undefined shape).
                    if all(isinstance(value, (list, tuple)) for value in values):
                        values_iterator = iter(values)
                        first_value_length: int = len(next(values_iterator))
                        current_value: Sized
                        if not all(
                            len(current_value) == first_value_length
                            for current_value in values_iterator
                        ):
                            return True

                    # Components of different types cannot be packaged into "numpy.ndarray" type (due to type mismatch).
                    values_iterator = iter(values)
                    first_value_type: type = type(next(values_iterator))
                    current_type: type
                    if not all(
                        type(current_value) == first_value_type
                        for current_value in values_iterator
                    ):
                        return True

                # Recursively evaluate each element of properly shaped iterable (list or tuple).
                for value in values:
                    if _detect_illegal_array_type_or_shape(values=value):
                        return True

        return False

    if _detect_illegal_array_type_or_shape(values=metric_values):
        return metric_values

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
        if self.metric_attributes is None:
            return ""

        return self.metric_attributes.to_id()

    @property
    def attributed_metric_values(self) -> Optional[Dict[str, MetricValue]]:
        if self.metric_values_by_batch_id is None or self.batch_ids is None:
            return None

        batch_id: str
        return {
            batch_id: self.metric_values_by_batch_id.get(batch_id)
            for batch_id in self.batch_ids
            if batch_id in self.metric_values_by_batch_id
        }

    @property
    def conditioned_attributed_metric_values(self) -> Dict[str, MetricValues]:
        if self.attributed_metric_values is None:
            return {}

        return AttributedResolvedMetrics.get_conditioned_attributed_metric_values_from_attributed_metric_values(
            attributed_metric_values=self.attributed_metric_values
        )

    @property
    def conditioned_metric_values(self) -> MetricValues:
        if self.attributed_metric_values is None:
            return None

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
