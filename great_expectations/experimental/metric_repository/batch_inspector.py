from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.metric_repository.metrics import (
    MetricRun,
)

if TYPE_CHECKING:
    from great_expectations import DataContext
    from great_expectations.datasource.fluent.interfaces import BatchRequest
    from great_expectations.experimental.metric_repository.metric_retriever import (
        MetricRetriever,
    )


class BatchInspector:
    # TODO: Docstrings
    def __init__(self, context: DataContext, metric_retrievers: list[MetricRetriever]):
        self._context = context
        self._metric_retrievers = metric_retrievers

    def compute_metric_run(self, batch_request: BatchRequest) -> MetricRun:
        run_id = self._generate_run_id()

        metrics = []
        for metric_retriever in self._metric_retrievers:
            metrics.extend(metric_retriever.get_metrics(batch_request=batch_request))

        return MetricRun(id=run_id, metrics=metrics)

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()
