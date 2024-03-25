from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, List, Optional

from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricRun,
    MetricTypes,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import BatchRequest
    from great_expectations.experimental.metric_repository.metric_retriever import (
        MetricRetriever,
    )


class BatchInspector:
    """A BatchInspector is responsible for computing metrics for a batch of data.

    It uses MetricRetriever objects to retrieve metrics.
    """

    def __init__(self, context: AbstractDataContext, metric_retrievers: list[MetricRetriever]):
        self._context = context
        self._metric_retrievers = metric_retrievers

    def compute_metric_list_run(
        self,
        data_asset_id: uuid.UUID,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]],
    ) -> MetricRun:
        """Method that computes a MetricRun for a list of metrics.
        Called by GX Agent to compute a MetricRun as part of a RunMetricsEvent.
        Args:
            data_asset_id (uuid.UUID): current data asset id.
            batch_request (BatchRequest): BatchRequest for current batch.
            metrics_list (Optional[List[MetricTypes]]): List of metrics to compute.
        Returns:
            MetricRun: _description_
        """
        # TODO: eventually we will keep this and retire `compute_metric_run`.
        metrics: list[Metric] = []
        for metric_retriever in self._metric_retrievers:
            metrics.extend(
                metric_retriever.get_metrics(batch_request=batch_request, metric_list=metric_list)
            )
        return MetricRun(data_asset_id=data_asset_id, metrics=metrics)

    def compute_metric_run(
        self, data_asset_id: uuid.UUID, batch_request: BatchRequest
    ) -> MetricRun:
        metrics: list[Metric] = []
        for metric_retriever in self._metric_retrievers:
            metrics.extend(metric_retriever.get_metrics(batch_request=batch_request))

        return MetricRun(data_asset_id=data_asset_id, metrics=metrics)

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()
