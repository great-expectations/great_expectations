import uuid

from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.column_descriptive_metrics.metrics import (
    BatchPointer,
    Metric,
    Metrics,
    Value,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator


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
        run_id: uuid.UUID,  # TODO: Should this be a separate type?
        raw_metric: int,  # TODO: What are the possible types of raw_metric?
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

        metric = Metric(
            id=self._generate_metric_id(),
            organization_id=self._organization_id,
            run_id=run_id,
            batch_pointer=BatchPointer(
                datasource_name=batch.datasource.name,
                data_asset_name=batch.data_asset.name,
                batch_name=batch.id,  # TODO: Where should the name come from?
            ),
            metric_name=metric_config.metric_name,
            metric_domain_kwargs=metric_config.metric_domain_kwargs,
            metric_value_kwargs=metric_config.metric_value_kwargs,
            column=metric_config.metric_domain_kwargs.get("column"),
            value=Value(value=raw_metric),
            details={},  # TODO: Pass details through
        )

        return metric


class AssetInspector:
    def __init__(self, organization_id: uuid.UUID):
        self._metric_converter = MetricConverter(organization_id=organization_id)

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def get_column_descriptive_metrics(self, batch: Batch) -> Metrics:
        run_id = self._generate_run_id()
        table_row_count = self._get_table_row_count_metric(batch=batch, run_id=run_id)
        metrics = Metrics(metrics=[table_row_count])
        return metrics

    def _get_table_row_count_metric(self, batch: Batch, run_id: uuid.UUID) -> Metric:
        metrics_calculator = MetricsCalculator(
            execution_engine=batch.datasource.get_execution_engine()
        )

        metric_config = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs={},
            metric_value_kwargs={},
        )

        raw_metric = metrics_calculator.get_metric(metric_config)

        metric = self._metric_converter.convert_raw_metric_to_metric_object(
            raw_metric=raw_metric,
            metric_config=metric_config,
            run_id=run_id,
            batch=batch,
        )

        return metric
