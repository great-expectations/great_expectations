import abc
import dataclasses
from typing import List, TypeVar, Generic, Optional

from typing_extensions import Protocol

from great_expectations.core.batch import BatchDataType
from great_expectations.execution_engine import ExecutionEngine


# The BatchRequestOptions provides extensibility for a specific execution engine.
# A particular execution engine can subclass BatchRequestOptions and add its relevant properties.
class BatchRequestOptions:
    pass


BatchRequestOptionsT = TypeVar("BatchRequestOptionsT", bound=BatchRequestOptions)


# BDIRKS BDIRKS I think we should have the datasource and and data asset here
#               We can define the names as properties.
@dataclasses.dataclass(frozen=True)
class BatchRequest(Generic[BatchRequestOptionsT]):
    # This is a collection of immutable parameters used to get data from the datasource/data asset
    # We plan to expand this to provide a mechanism to add request parameters that exist for all
    # datasources.
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptionsT

    # Usage stats expects a method like this to exist but I'm not sure if we really want to
    # implement it or not. Other changes would also be necessary. I've commented it out while
    # I sort through the usage stats piece.
    # def to_json_dict(self) -> dict:
    #     # Convert the BatchRequest to a dictionary that is json serializable
    #     # NOT a JSON string.
    #     # This is necessary for usage stats.
    #     return dataclasses.asdict(self)


class DataAsset(abc.ABC, Generic[BatchRequestOptionsT]):
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    # BDIRKS not sure if quite right.
    def get_batch_request(self, options: Optional[BatchRequestOptionsT]) -> BatchRequest[BatchRequestOptionsT]:
        ...


class Datasource(Protocol):
    execution_engine: ExecutionEngine
    asset_types: List[type]
    name: str

    def get_batch_list_from_batch_request(
            self, batch_request: "BatchRequest"
    ) -> "List[Batch]":
        """Processes a batch request and returns a list of batches.

        Args:
            batch_request: contains parameters necessary to retrieve batches.

        Returns:
            A list of batches. The list may be empty.
        """

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""


class Batch(Generic[BatchRequestOptionsT]):
    # This represents a batch of data
    def __init__(
            self,
            datasource: Datasource,
            data_asset: DataAsset,
            batch_request: BatchRequest[BatchRequestOptionsT],
            data: BatchDataType,  # BDIRKS, this needs to be make more generic? Maybe users can subclass
            # BatchData or maybe introduce a protocol.
    ):
        """This represents a batch of data.

        This is usually not the data itself but a hook to the data on an external datastore such as
        a spark or a sql database. An exception exists for pandas or any in-memory datastore.
        """
        # These properties are intended to be READ-ONLY
        self._datasource: Datasource = datasource
        self._data_asset: DataAsset = data_asset
        self._batch_request: BatchRequest[BatchRequestOptionsT] = batch_request
        self._data: BatchDataType = data

        # computed property
        # We need to unique identifier. This will likely change as I get more input
        self._id: str = "-".join(
            [datasource.name, data_asset.name, str(batch_request)]
        )

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    @property
    def batch_request(self) -> BatchRequest[BatchRequestOptionsT]:
        return self._batch_request

    @property
    def id(self) -> str:
        return self._id

    @property
    def data(self) -> BatchDataType:
        return self._data

    @property
    def execution_engine(self):
        return self.datasource.execution_engine
