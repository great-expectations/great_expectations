import logging
from collections import OrderedDict
from typing import Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    Batch,
    BatchData,
    BatchDefinition,
    BatchMarkers,
)
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class BatchCache:
    def __init__(
        self,
        execution_engine: "ExecutionEngine",  # noqa: F821
        batch_list: Optional[List[Batch]] = None,
    ) -> None:
        """
        Args:
            execution_engine: The ExecutionEngine to be used to access cache of loaded Batch objects.
            batch_list: List of Batch objects available from external source (default is None).
        """
        self._execution_engine = execution_engine

        self._active_batch_id = None

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
    def execution_engine(self) -> "ExecutionEngine":  # noqa: F821
        return self._execution_engine

    @property
    def batches(self) -> Dict[str, Batch]:
        """Getter for ordered dictionary (cache) of "Batch" objects in use (with batch_id as key)."""
        if not isinstance(self._batches, OrderedDict):
            self._batches = OrderedDict(self._batches)

        return self._batches

    @property
    def loaded_batch_ids(self) -> List[str]:
        return list(self.batches.keys())

    @property
    def active_batch_id(self) -> Optional[str]:
        """The batch id for the default batch data.

        When a specific Batch objec is unavailable, then the data associated with the active_batch_id will be used.
        """
        if self._active_batch_id is not None:
            return self._active_batch_id

        if len(self.batches) == 1:
            return list(self.batches.keys())[0]

        return None

    @active_batch_id.setter
    def active_batch_id(self, batch_id: str) -> None:
        if batch_id in self.batches.keys():
            self._active_batch_id = batch_id
        else:
            raise ge_exceptions.InvalidBatchIdError(
                f"Unable to set active_batch_data_id to {batch_id}.  The data may not be loaded."
            )

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

    def reset(self) -> None:
        """Clears Batch cache"""
        self._batches = {}

    def load_batch_list(self, batch_list: List[Batch]) -> None:
        if batch_list is None:
            batch_list = []

        batch: Batch
        for batch in batch_list:
            try:
                assert isinstance(
                    batch, Batch
                ), "Batch objects provided to BatchCache must be formal Great Expectations Batch typed objects."
            except AssertionError as e:
                logger.warning(str(e))

            self._batches[batch.id] = batch
            self._execution_engine.load_batch_data(
                batch_id=batch.id, batch_data=batch.data
            )

    def save_batch_data(self, batch_id: str, batch_data: BatchData) -> None:
        """
        Updates the data for the specified Batch in the cache
        """
        if batch_id not in self._batches:
            self._batches[batch_id] = Batch(data=batch_data)
        else:
            self._batches[batch_id].data = batch_data

        self.active_batch_id = batch_id
