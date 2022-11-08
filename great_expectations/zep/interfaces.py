from __future__ import annotations

import abc
import dataclasses
from typing import Any, Dict, List, Mapping, Optional, Type

from typing_extensions import ClassVar, TypeAlias

from great_expectations.core.batch import BatchDataType
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.zep.metadatasource import MetaDatasource

# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants returned. In the simple case the keys represent dimensions one can
# slice the data along and the values are the values. One can also namespace these key/value
# pairs, hence the Dict[str, BatchRequestValue], allowed values. For example:
#   options = {
#       "month": "3"
#       "year_splitter": {
#           "year": "2020"
#       }
#    }
# The month key is in the global namespace while the year key is in the year_splitter namespace.
BatchRequestOptions: TypeAlias = Dict[str, Any]


@dataclasses.dataclass(frozen=True)
class BatchRequest:
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptions


class DataAsset(abc.ABC):
    _name: str

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @abc.abstractmethod
    def get_batch_request(self, options: Optional[BatchRequestOptions]) -> BatchRequest:
        ...


class Datasource(metaclass=MetaDatasource):
    # class attrs
    asset_types: ClassVar[List[Type[DataAsset]]] = []

    # instance attrs
    name: str
    execution_engine: ExecutionEngine
    assets: Mapping[str, DataAsset]

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
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


class Batch:
    # Instance variable declarations
    _datasource: Datasource
    _data_asset: DataAsset
    _batch_request: BatchRequest
    _data: BatchDataType
    _id: str

    def __init__(
        self,
        datasource: Datasource,
        data_asset: DataAsset,
        batch_request: BatchRequest,
        # BatchDataType is Union[core.batch.BatchData, pd.DataFrame, SparkDataFrame].  core.batch.Batchdata is the
        # implicit interface that Datasource implementers can use. We can make this explicit if needed.
        data: BatchDataType,
    ) -> None:
        """This represents a batch of data.

        This is usually not the data itself but a hook to the data on an external datastore such as
        a spark or a sql database. An exception exists for pandas or any in-memory datastore.
        """
        # These properties are intended to be READ-ONLY
        self._datasource: Datasource = datasource
        self._data_asset: DataAsset = data_asset
        self._batch_request: BatchRequest = batch_request
        self._data: BatchDataType = data

        # computed property
        # We need to unique identifier. This will likely change as I get more input
        self._id: str = "-".join([datasource.name, data_asset.name, str(batch_request)])

    @property
    def datasource(self) -> Datasource:
        return self._datasource

    @property
    def data_asset(self) -> DataAsset:
        return self._data_asset

    @property
    def batch_request(self) -> BatchRequest:
        return self._batch_request

    @property
    def id(self) -> str:
        return self._id

    @property
    def data(self) -> BatchDataType:
        return self._data

    @property
    def execution_engine(self) -> ExecutionEngine:
        return self.datasource.execution_engine
