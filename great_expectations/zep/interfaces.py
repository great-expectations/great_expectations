import abc
from typing import Dict, List, Type, Union

from typing_extensions import ClassVar

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.metadatasource import MetaDatasource


class DataAsset(abc.ABC):
    name: str

    def __init__(self, name: str) -> None:
        self.name = name


class Datasource(metaclass=MetaDatasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    # instance attrs
    name: str
    execution_engine: ExecutionEngine
    assets: Dict[str, DataAsset]

    def get_batch_list_from_batch_request(
        self, batch_request: Union[BatchRequest, RuntimeBatchRequest]
    ) -> List[Batch]:
        """Processes a batch request and returns a list of batches.

        Args:
            batch_request: contains parameters necessary to retrieve batches.

        Returns:
            A list of batches. The list may be empty.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement `.get_batch_list_from_batch_request()`"
        )

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        return self.assets[asset_name]
