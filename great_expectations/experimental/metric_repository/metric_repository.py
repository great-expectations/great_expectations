from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import Metrics


class MetricRepository:
    pass
    # TODO: Add methods, docstrings

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def add(self, metrics: Metrics) -> Metrics:
        return self._data_store.add(value=metrics)
