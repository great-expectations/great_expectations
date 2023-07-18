from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.column_descriptive_metrics.metrics import (
    Metric,
    Metrics,
    Value,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch
    from great_expectations.validator.metric_configuration import MetricConfiguration


class MetricConverter:  # TODO: Name this better
    def __init__(self, organization_id: uuid.UUID):
        self._organization_id = organization_id

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def convert_metrics_dict_to_metrics_object(self, raw_metrics: dict) -> Metrics:
        """Convert a dict of metrics to a Metrics object.
        Args:
            raw_metrics: Dict of metrics, where keys are metric names and values are metrics.
                Generated by the MetricsCalculator.
        Returns:
            Metrics object.
        """
        # TODO: Implementation (if needed)
        raise NotImplementedError

    def convert_raw_metric_to_metric_object(
        self,
        batch: Batch,
        run_id: uuid.UUID,  # TODO: Should run_id be a separate type?
        raw_metric: int | list,  # TODO: What are the possible types of raw_metric?
        metric_config: MetricConfiguration,
    ) -> Metric:
        """Convert a dict of a single metric to a Metric object.
        Args:
            raw_metric: Dict of a single metric, where keys are metric names and values are metrics.
                Generated by the MetricsCalculator.
            metric_config: MetricConfiguration object for this metric.
        Returns:
            Metric object.
        """
        # TODO: Add the rest of the metric fields, convert value to Value object:

        print("converting metric dict to metric object")

        # TODO: Consider just having Batch as a parameter and serializing the parts we want
        #  (e.g. datasource_name, data_asset_name, batch_id).
        metric = Metric(
            id=self._generate_metric_id(),
            organization_id=self._organization_id,
            run_id=run_id,
            batch=batch,
            metric_name=metric_config.metric_name,
            metric_domain_kwargs=metric_config.metric_domain_kwargs,
            metric_value_kwargs=metric_config.metric_value_kwargs,
            column=metric_config.metric_domain_kwargs.get("column"),
            value=Value(value=raw_metric),
            details={},  # TODO: Pass details through
        )

        return metric
