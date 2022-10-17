import logging
from collections import OrderedDict
from typing import Dict, List, Optional

from great_expectations.core.batch import (
    Batch,
    BatchData,
    BatchDefinition,
    BatchMarkers,
)
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class BatchManager:
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
        self._active_batch_data_id = None

        if batch_list is None:
            batch_list = []

        self._batch_cache = {}
        self._batch_data_cache = {}

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
    def batch_data_cache(self) -> Dict[str, BatchData]:
        """Dictionary of loaded BatchData objects."""
        return self._batch_data_cache

    @property
    def loaded_batch_ids(self) -> List[str]:
        """IDs of loaded BatchData objects."""
        return list(self._batch_data_cache.keys())

    @property
    def active_batch_data_id(self) -> Optional[str]:
        """The batch id for the default batch data.

        When a specific Batch objec is unavailable, then the data associated with the active_batch_data_id will be used.
        """
        if self._active_batch_data_id is not None:
            return self._active_batch_data_id

        if len(self._batch_data_cache) == 1:
            return list(self._batch_data_cache.keys())[0]

        return None

    @property
    def active_batch_data(self) -> Optional[BatchData]:
        """The BatchData object from the currently-active Batch object."""
        if self.active_batch_data_id is None:
            return None

        return self._batch_data_cache.get(self.active_batch_data_id)

    @property
    def batch_cache(self) -> Dict[str, Batch]:
        """Getter for ordered dictionary (cache) of "Batch" objects in use (with batch_id as key)."""
        if not isinstance(self._batch_cache, OrderedDict):
            self._batch_cache = OrderedDict(self._batch_cache)

        return self._batch_cache

    @property
    def active_batch_id(self) -> Optional[str]:
        """Getter for active Batch ID"""
        active_batch_data_id: str = self._active_batch_data_id
        if active_batch_data_id != self._active_batch_id:
            logger.warning(
                "ID of active Batch and ID of active loaded BatchData differ."
            )

        return self._active_batch_id

    @property
    def active_batch(self) -> Optional[Batch]:
        """Getter for active Batch"""
        active_batch_id: Optional[str] = self.active_batch_id
        batch: Optional[Batch] = (
            None if active_batch_id is None else self.batch_cache.get(active_batch_id)
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

    def reset_batch_cache(self) -> None:
        """Clears Batch cache"""
        self._batch_cache = {}
        self._active_batch_id = None

    def load_batch_list(self, batch_list: List[Batch]) -> None:
        if batch_list is None:
            batch_list = []

        batch: Batch
        for batch in batch_list:
            try:
                assert isinstance(
                    batch, Batch
                ), "Batch objects provided to BatchManager must be formal Great Expectations Batch typed objects."
            except AssertionError as e:
                logger.warning(str(e))

            self._execution_engine.load_batch_data(
                batch_id=batch.id, batch_data=batch.data
            )

            self._batch_cache[batch.id] = batch
            # We set the active_batch_id in each iteration of the loop to keep in sync with the active_batch_data_id
            # that has been loaded.  Hence, the final active_batch_id will be that of the final BatchData loaded.
            self._active_batch_id = batch.id

    def save_batch_data(self, batch_id: str, batch_data: BatchData) -> None:
        """
        Updates the data for the specified Batch in the cache
        """
        self._batch_data_cache[batch_id] = batch_data
        self._active_batch_data_id = batch_id
