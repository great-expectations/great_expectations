from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.experimental.metric_repository.metrics import Metric


class MetricRetriever(abc.ABC):
    """A MetricRetriever is responsible for retrieving metrics for a batch of data."""

    def __init__(self, context: AbstractDataContext):
        self._context = context

    @abc.abstractmethod
    def get_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        raise NotImplementedError

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()
