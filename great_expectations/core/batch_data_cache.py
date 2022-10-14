import logging
from typing import Dict, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchData

logger = logging.getLogger(__name__)


class BatchDataCache:
    def __init__(self) -> None:
        self._batch_data_dict = {}
        self._active_batch_data_id = None

    @property
    def active_batch_data_id(self) -> Optional[str]:
        """The batch id for the default batch data.

        When an execution engine is asked to process a compute domain that does
        not include a specific batch_id, then the data associated with the
        active_batch_data_id will be used as the default.
        """
        if self._active_batch_data_id is not None:
            return self._active_batch_data_id

        if len(self.batch_data_dict) == 1:
            return list(self.batch_data_dict.keys())[0]

        return None

    @active_batch_data_id.setter
    def active_batch_data_id(self, batch_id: str) -> None:
        if batch_id in self.batch_data_dict.keys():
            self._active_batch_data_id = batch_id
        else:
            raise ge_exceptions.ExecutionEngineError(
                f"Unable to set active_batch_data_id to {batch_id}.  The data may not be loaded."
            )

    @property
    def active_batch_data(self) -> Optional[BatchData]:
        """The data from the currently-active Batch object."""
        if self.active_batch_data_id is None:
            return None

        return self.batch_data_dict.get(self.active_batch_data_id)

    @property
    def batch_data_dict(self) -> Dict[str, BatchData]:
        """The current dictionary of Batch data objects."""
        return self._batch_data_dict

    @property
    def batch_data_ids(self) -> List[str]:
        return list(self.batch_data_dict.keys())

    def save_batch_data(self, batch_id: str, batch_data: BatchData) -> None:
        """
        Adds the specified batch_data to the cache
        """
        print(
            f"\n[ALEX_TEST] [BATCH_DATA_CACHE.SAVE_BATCH_DATA] BATCH_ID-0:\n{batch_id} ; TYPE: {str(type(batch_id))}"
        )
        print(
            f"\n[ALEX_TEST] [BATCH_DATA_CACHE.SAVE_BATCH_DATA] BATCH_DATA:\n{batch_data} ; TYPE: {str(type(batch_data))}"
        )
        self._batch_data_dict[batch_id] = batch_data
        self._active_batch_data_id = batch_id
        print(
            f"\n[ALEX_TEST] [BATCH_DATA_CACHE.SAVE_BATCH_DATA] BATCH_ID-1:\n{batch_id} ; TYPE: {str(type(batch_id))}"
        )
