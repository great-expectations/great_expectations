from dataclasses import dataclass
from typing import List, TypeVar, Generic

from typing_extensions import Protocol

from great_expectations.core.batch import BatchDataType
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine import ExecutionEngine


class DataAsset:
    # Gabriel is adding this
    pass


class Datasource(Protocol):
    execution_engine: ExecutionEngine
    asset_types: List[type]

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


# The BatchSpec provides extensibility for a specific execution engine.
# A particular execution engine can subclass BatchSpec and add its relevant properties.
# Currently, this is a special dict that should be put under stronger typing
BatchSpecT = TypeVar("BatchSpecT", bound=BatchSpec)


@dataclass(frozen=True)
class BatchRequest(Generic[BatchSpecT]):
    # This is a collection of immutable parameters used to get data from the datasource/data asset
    # We plan to expand this to provide a mechanism to add request parameters that exist for all
    # datasources.
    datasource_name: str
    data_asset_name: str
    batch_spec: BatchSpecT


class Batch(Generic[BatchSpecT]):
    # This represents a batch of data
    def __init__(
            self,
            datasource: Datasource,
            data_asset: DataAsset,  # BDIRKS, this needs to be incorporated from Gabriel's changes.
            batch_request: BatchRequest[BatchSpecT],
            data: BatchDataType,   # BDIRKS, this needs to be make more generic? Maybe users can subclass
                                   # BatchData or maybe introduce a protocol.
    ):
        """This represents a batch of data.

        This is usually not the data itself but a hook to the data on an external datastore such as
        a spark or a sql database. An exception exists for pandas or any in-memory datastore.
        """
        # These properties are intended to be READ-ONLY
        self._datasource: Datasource = datasource
        self._data_asset: DataAsset = data_asset
        self._batch_request: BatchRequest[BatchSpecT] = batch_request
        self._data: BatchDataType = data

        # computed property
        self._id: str = batch_request.batch_spec.to_id()

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    @property
    def batch_request(self) -> BatchRequest[BatchSpecT]:
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

