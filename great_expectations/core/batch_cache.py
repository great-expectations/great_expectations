import logging
from collections import OrderedDict
from typing import Dict, List, Optional, Union

from great_expectations.core.batch import (
    Batch,
    BatchData,
    BatchDefinition,
    BatchMarkers,
    SparkDataFrame,
)
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)
logging.captureWarnings(True)

try:
    import pandas as pd
except ImportError:
    pd = None

    logger.debug(
        "Unable to load pandas; install optional pandas dependency for support."
    )


class BatchCache:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        batch_list: Optional[List[Batch]] = None,
    ) -> None:
        """
        Args:
            execution_engine: The ExecutionEngine to be used to access cache of loaded BatchData objects.
            batch_list: List of Batch objects available from external source (default is None).
        """
        self._execution_engine = execution_engine

        if batch_list is None:
            batch_list = []

        self._batches = {}

        self.load_batch_list(batch_list=batch_list)
        if len(batch_list) > 1:
            logger.debug(
                f"{len(batch_list)} batches will be added to this Validator.  The batch_identifiers for the active "
                f"batch are {self.active_batch.batch_definition['batch_identifiers'].items()}"
            )

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self._execution_engine

    @property
    def batches(self) -> Dict[str, Batch]:
        """Getter for ordered dictionary (cache) of "Batch" objects in use (with batch_id as key)."""
        if not isinstance(self._batches, OrderedDict):
            self._batches = OrderedDict(self._batches)

        return self._batches

    @property
    def loaded_batch_ids(self) -> List[str]:
        return self._execution_engine.batch_data_cache.batch_data_ids

    @property
    def active_batch_id(self) -> Optional[str]:
        """Getter for active batch id"""
        return self._execution_engine.batch_data_cache.active_batch_data_id

    @property
    def active_batch(self) -> Optional[Batch]:
        """Getter for active batch"""
        active_batch_id: Optional[str] = self.active_batch_id
        batch: Optional[Batch] = (
            None if active_batch_id is None else self.batches.get(active_batch_id)
        )
        return batch

    @property
    def active_batch_spec(self) -> Optional[BatchSpec]:
        """Getter for active batch's batch_spec"""
        if not self.active_batch:
            return None
        else:
            return self.active_batch.batch_spec

    @property
    def active_batch_markers(self) -> Optional[BatchMarkers]:
        """Getter for active batch's batch markers"""
        if not self.active_batch:
            return None
        else:
            return self.active_batch.batch_markers

    @property
    def active_batch_definition(self) -> Optional[BatchDefinition]:
        """Getter for the active batch's batch definition"""
        if not self.active_batch:
            return None
        else:
            return self.active_batch.batch_definition

    def load_batch_list(self, batch_list: List[Batch]) -> None:
        batch_data_dict: Dict[str, Union[BatchData, pd.DataFrame, SparkDataFrame]] = {}

        batch: Batch
        for batch in batch_list:
            try:
                assert isinstance(
                    batch, Batch
                ), "Batch objects provided to BatchCache must be formal Great Expectations Batch typed objects."
            except AssertionError as e:
                logger.warning(str(e))

            self._batches[batch.id] = batch
            batch_data_dict[batch.id] = batch.data

        self._execution_engine._load_batch_data_from_dict(
            batch_data_dict=batch_data_dict
        )
