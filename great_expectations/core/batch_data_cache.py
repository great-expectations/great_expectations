import logging
from typing import Dict, List, Optional, Union

import pandas as pd

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchData

logger = logging.getLogger(__name__)

try:
    import pyspark
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    pyspark = None
    SparkDataFrame = None
    logger.debug(
        "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes"
    )


class BatchDataCache:
    def __init__(
        self,
        batch_data_dict: Optional[
            Dict[str, Union[BatchData, pd.DataFrame, SparkDataFrame]]
        ] = None,
    ) -> None:
        if batch_data_dict is None:
            batch_data_dict = {}

        self._batch_data_dict = {}

        self._active_batch_data_id = None
        self._load_batch_data_from_dict(batch_data_dict=batch_data_dict)

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
    def active_batch_data(
        self,
    ) -> Optional[Union[BatchData, pd.DataFrame, SparkDataFrame]]:
        """The data from the currently-active Batch object."""
        if self.active_batch_data_id is None:
            return None

        # TODO: <Alex>ALEX</Alex>
        a = self.batch_data_dict.get(self.active_batch_data_id)
        print(
            f"\n[ALEX_TEST] [BATCH_DATA_CACHE.active_batch_data] RETURNING_PROPERTY:\n{a} ; TYPE: {str(type(a))}"
        )
        # TODO: <Alex>ALEX</Alex>
        return self.batch_data_dict.get(self.active_batch_data_id)

    @property
    def batch_data_dict(
        self,
    ) -> Dict[str, Union[BatchData, pd.DataFrame, SparkDataFrame]]:
        """The current dictionary of Batch data objects."""
        return self._batch_data_dict

    @property
    def batch_data_ids(self) -> List[str]:
        return list(self.batch_data_dict.keys())

    def load_batch_data(
        self, batch_id: str, batch_data: Union[BatchData, pd.DataFrame, SparkDataFrame]
    ) -> None:
        """
        Adds the specified batch_data to the cache
        """
        self._batch_data_dict[batch_id] = batch_data
        self._active_batch_data_id = batch_id

    def _load_batch_data_from_dict(
        self, batch_data_dict: Dict[str, Union[BatchData, pd.DataFrame, SparkDataFrame]]
    ) -> None:
        """
        Loads all data in batch_data_dict using cache_batch_data
        """
        batch_id: str
        batch_data: Union[BatchData, pd.DataFrame, SparkDataFrame]
        for batch_id, batch_data in batch_data_dict.items():
            print(
                f"\n[ALEX_TEST] [BATCH_DATA_CACHE._load_batch_data_from_dict()] BATCH_DATA:\n{batch_data} ; TYPE: {str(type(batch_data))}"
            )
            self.load_batch_data(batch_id=batch_id, batch_data=batch_data)
