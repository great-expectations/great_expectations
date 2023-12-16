from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from great_expectations.experimental.metric_repository.data_store import DataStore
    from great_expectations.experimental.metric_repository.metrics import MetricRun


class MetricRepository:
    """A repository for storing and retrieving MetricRuns.

    Args:
        data_store: The DataStore to use for storing and retrieving MetricRuns.
    """

    def __init__(self, data_store: DataStore):
        self._data_store = data_store

    def add_metric_run(self, metric_run: MetricRun) -> uuid.UUID:
        return self._data_store.add(value=metric_run)
