import logging
from abc import ABC
from dataclasses import dataclass
from typing import Dict, List, Optional
from typing_extensions import Self

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.datasource.misc_types import NewBatchRequestBase
from great_expectations.datasource.new_new_new_datasource import NewNewNewDatasource
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)

class BaseDataAsset:
    def __init__(
        self,
        datasource,  # Should be of type: V15Datasource
        name: str,
        batch_identifiers: List[str],
    ) -> None:
        self._datasource = datasource
        self._name = name
        self._batch_identifiers = batch_identifiers

    @property
    def datasource(self) -> NewNewNewDatasource:
        return self._datasource

    @property
    def name(self) -> str:
        return self._name

    @property
    def batch_identifiers(self) -> List[str]:
        return self._batch_identifiers

    def set_name(self, name: str):
        """Changes the DataAsset's name.

        Note: This method is intended to be called only from Dataasource's rename_asset method.
        This will keep the name of the asset in sync with the key in the Datasource's _asset registry.
        """
        self._name = name

    def update_configuration(self, **kwargs) -> Self:
        raise NotImplementedError
        
    def list_batches(self) -> List[NewBatchRequestBase]:
        raise NotImplementedError

    def get_batch(self, *args, **kwargs) -> Batch:
        raise NotImplementedError

    def get_batches(self, *args, **kwargs) -> List[Batch]:
        raise NotImplementedError

    def get_batch_request(self, *args, **kwargs) -> NewBatchRequestBase:
        raise NotImplementedError

    def get_validator(self, *args, **kwargs) -> Validator:
        raise NotImplementedError

    def __eq__(self, other) -> bool:
        # !!! I'm not sure if this is a good implementation of __eq__, but I had to do something to get `assert A == B` in tests working.

        return all(
            [
                self._datasource == other._datasource,
                self._name == other._name,
                self._batch_identifiers == other._batch_identifiers,
            ]
        )

    def __str__(self):
        # !!! We should figure out a convention for __str__ifying objects, and apply it across the codebase
        return f"""great_expectations.datasource.runtime_pandas_data_asset.RuntimePandasDataAsset object :
    datasource:        {self._datasource}
    name:              {self._name}
    batch_identifiers: {self._batch_identifiers}
"""
