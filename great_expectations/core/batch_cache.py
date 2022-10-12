import logging
from collections import OrderedDict
from typing import Dict, List, Optional

from great_expectations.core.batch import Batch, BatchDefinition, BatchMarkers
from great_expectations.core.batch_data_cache import BatchDataCache
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class BatchCache:
    def __init__(
        self,
        batch_data_cache: BatchDataCache,
        batch_list: Optional[List[Batch]] = None,
    ) -> None:
        """
        Args:
            batch_data_cache: Cache of BatchData objects ("batch_data" is property of "Batch") loaded.
            batch_list: List of Batch objects available from external source (default is None).
        """
        self._batch_data_cache = batch_data_cache

        if batch_list is None:
            batch_list = []

        self._batch_list = {}

        self.load_batch_list(batch_list=batch_list)
        if len(batch_list) > 1:
            logger.debug(
                f"{len(batch_list)} batches will be added to this Validator.  The batch_identifiers for the active "
                f"batch are {self.active_batch.batch_definition['batch_identifiers'].items()}"
            )

    @property
    def batch_list(self) -> Dict[str, Batch]:
        """Getter for batch_list"""
        if not isinstance(self._batch_list, OrderedDict):
            self._batch_list = OrderedDict(self._batch_list)

        return self._batch_list

    @property
    def loaded_batch_ids(self) -> List[str]:
        return self._batch_data_cache.batch_data_ids

    @property
    def active_batch_id(self) -> Optional[str]:
        """Getter for active batch id"""
        return self._batch_data_cache.active_batch_data_id

    @property
    def active_batch(self) -> Optional[Batch]:
        """Getter for active batch"""
        active_batch_id: Optional[str] = self.active_batch_id
        batch: Optional[Batch] = (
            self.batch_list.get(active_batch_id) if active_batch_id else None
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
        batch: Batch
        for batch in batch_list:
            try:
                assert isinstance(
                    batch, Batch
                ), "Batch objects provided to BatchCache must be formal Great Expectations Batch typed objects."
            except AssertionError as e:
                logger.warning(str(e))

            self._batch_data_cache.save_batch_data(
                batch_id=batch.id, batch_data=batch.data
            )
            self._batch_list[batch.id] = batch
