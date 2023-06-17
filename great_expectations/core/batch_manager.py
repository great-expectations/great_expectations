from __future__ import annotations

import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Dict, List, Optional

from great_expectations.core.batch import (
    Batch,
    BatchDataType,
    BatchDefinition,
    BatchMarkers,
    _get_fluent_batch_class,
)

if TYPE_CHECKING:
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class BatchManager:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        batch_list: Optional[List[Batch]] = None,
    ) -> None:
        """
        Args:
            execution_engine: The ExecutionEngine to be used to access cache of loaded Batch objects.
            batch_list: List of Batch objects available from external source (default is None).
        """
        self._execution_engine: ExecutionEngine = execution_engine

        self._active_batch_id: Optional[str] = None
        self._active_batch_data_id: Optional[str] = None

        self._batch_cache: Dict[str, Batch] = OrderedDict()
        self._batch_data_cache: Dict[str, BatchDataType] = {}

        if batch_list:
            self.load_batch_list(batch_list=batch_list)

    @property
    def batch_data_cache(self) -> Dict[str, BatchDataType]:
        """Dictionary of loaded BatchData objects."""
        return self._batch_data_cache

    @property
    def loaded_batch_ids(self) -> List[str]:
        """IDs of loaded BatchData objects."""
        return list(self._batch_data_cache.keys())

    @property
    def active_batch_data_id(self) -> Optional[str]:
        """
        The Batch ID for the default "BatchData" object.

        When a specific Batch is unavailable, then the data associated with the active_batch_data_id will be used.

        This is a "safety valve" provision.  If self._active_batch_data_id is unavailable (e.g., did not get set for
        some reason), then if there is exactly and unambiguously one loaded "BatchData" object, then it will play the
        role of the "active_batch_data_id", which is needed to compute a metric (by the particular ExecutionEngine).
        However, if there is more than one, then "active_batch_data_id" becomes ambiguous, and thus "None" is returned.
        """
        if self._active_batch_data_id is not None:
            return self._active_batch_data_id

        if len(self._batch_data_cache) == 1:
            return list(self._batch_data_cache.keys())[0]

        return None

    @property
    def active_batch_data(self) -> Optional[BatchDataType]:
        """The BatchData object from the currently-active Batch object."""
        if self.active_batch_data_id is None:
            return None

        return self._batch_data_cache.get(self.active_batch_data_id)

    @property
    def batch_cache(self) -> Dict[str, Batch]:
        """Getter for ordered dictionary (cache) of "Batch" objects in use (with batch_id as key)."""
        return self._batch_cache

    @property
    def active_batch_id(self) -> Optional[str]:
        """
        Getter for active Batch ID.

        Indeed, "active_batch_data_id" and "active_batch_id" can be different.  The former refers to the most recently
        loaded "BatchData" object, while the latter refers to the most recently requested "Batch" object.  In applicable
        situations, no new "BatchData" objects have been loaded; however, a new "Validator" object was instantiated with
        the list of "Batch" objects, each of whose BatchData has already been loaded (and cached).  Since BatchData IDs
        are from the same name space as Batch IDs, this helps avoid unnecessary loading of data from different backends.
        """
        active_batch_data_id: Optional[str] = self.active_batch_data_id
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

        return self.active_batch.batch_spec

    @property
    def active_batch_markers(self) -> Optional[BatchMarkers]:
        """Getter for active batch's batch markers"""
        if not self.active_batch:
            return None

        return self.active_batch.batch_markers

    @property
    def active_batch_definition(self) -> Optional[BatchDefinition]:
        """Getter for the active batch's batch definition"""
        if not self.active_batch:
            return None

        return self.active_batch.batch_definition

    def reset_batch_cache(self) -> None:
        """Clears Batch cache"""
        self._batch_cache = OrderedDict()
        self._active_batch_id = None

    def load_batch_list(self, batch_list: List[Batch]) -> None:
        batch: Batch
        for batch in batch_list:
            try:
                assert isinstance(
                    batch, (Batch, _get_fluent_batch_class())
                ), "Batch objects provided to BatchManager must be formal Great Expectations Batch typed objects."
            except AssertionError as e:
                logger.error(str(e))

            self._execution_engine.load_batch_data(
                batch_id=batch.id, batch_data=batch.data  # type: ignore[arg-type] # batch.data could be None
            )

            self._batch_cache[batch.id] = batch
            # We set the active_batch_id in each iteration of the loop to keep in sync with the active_batch_data_id
            # that has been loaded.  Hence, the final active_batch_id will be that of the final BatchData loaded.
            self._active_batch_id = batch.id

    def save_batch_data(self, batch_id: str, batch_data: BatchDataType) -> None:
        """
        Updates the data for the specified Batch in the cache
        """
        self._batch_data_cache[batch_id] = batch_data
        self._active_batch_data_id = batch_id
