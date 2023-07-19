from great_expectations.experimental.column_descriptive_metrics.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.column_descriptive_metrics.metrics import Metrics


class MetricRepository:
    pass
    # TODO: Add methods

    def __init__(self, data_store: CloudDataStore):
        self._data_store = data_store

    def create(self, metrics: Metrics) -> None:
        print("Creating metric in ColumnDescriptiveMetricsRepository")
        self._data_store.create(
            value_type=Metrics, value=metrics
        )  # TODO: How to annotate/implement?
