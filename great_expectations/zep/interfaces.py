import abc
from typing import Dict, List, Type, Union

from typing_extensions import Protocol, runtime_checkable

from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.execution_engine import ExecutionEngine


class DataAsset(abc.ABC):
    name: str

    def __init__(self, name: str) -> None:
        self.name = name


@runtime_checkable
class Datasource(Protocol):
    execution_engine: ExecutionEngine
    asset_types: List[Type[DataAsset]]
    name: str
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

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        return self.assets[asset_name]
