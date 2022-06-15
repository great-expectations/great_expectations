from abc import ABC
from dataclasses import dataclass
import logging
from typing import List, Optional, Dict

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.types import SerializableDictDot
from great_expectations.validator.validator import Validator

# from great_expectations.datasource.pandas_reader_datasource import PandasReaderDatasource #!!! This causes a circular import

logger = logging.getLogger(__name__)


class BatchIdentifierException(BaseException):
    # !!! Do we want to create a class for this? Is this the right name and inheritance?
    pass



### Second approach to BatchRequests ###


# !!! I kinda hate this name
class DataConnectorQuery(dict, SerializableDictDot):
    pass

# !!! I kinda hate this name
class BatchSpecPassthrough(dict, SerializableDictDot):
    pass

@dataclass
class NewBatchRequestBase(SerializableDictDot, ABC):

    datasource_name: str
    data_asset_name: str
    data_connector_query: DataConnectorQuery

@dataclass
class NewConfiguredBatchRequest(NewBatchRequestBase):
    batch_spec_passthrough: BatchSpecPassthrough

@dataclass
class NewRuntimeBatchRequest(NewBatchRequestBase):
    data: BatchSpecPassthrough



### First approach to BatchRequests ###

class BatchIdentifiers(dict):
    pass

class RuntimeParameters(dict):
    pass

class NewBatchRequest:
    # !!! This prolly isn't the right name for this class

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        batch_identifiers: Optional[BatchIdentifiers] = None,
        runtime_parameters: Optional[RuntimeParameters] = None,
    ) -> None:
        self._datasource_name = datasource_name
        self._data_asset_name = data_asset_name

        self._batch_identifiers = batch_identifiers
        self._runtime_parameters = runtime_parameters

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def batch_identifiers(self) -> str:
        return self._batch_identifiers

    @property
    def runtime_parameters(self) -> str:
        return self._runtime_parameters

    def __str__(self) -> str:
        # !!! This isn't right---very slapdash
        return f"""{self.datasource_name},{self.data_asset_name},{self._batch_identifiers},{self._runtime_parameters}"""

    def __eq__(self, other) -> bool:
        # !!! I'm not sure if this is a good implementation of __eq__, but I had to do something to get `assert A == B` in tests working.

        return all([
           self.datasource_name == other.datasource_name,
           self.data_asset_name == other.data_asset_name,
           self.batch_identifiers == other.batch_identifiers,
           self.runtime_parameters == other.runtime_parameters,
        ])



class BaseDataAsset:

    def __init__(
        self,
        datasource, #Should be of type: V15Datasource
        name: str,
        batch_identifiers: List[str]
    ) -> None:
        self._datasource = datasource
        self._name = name
        self._batch_identifiers = batch_identifiers

    @property
    def name(self) -> str:
        return self._name

    @property
    def batch_identifiers(self) -> List[str]:
        return self._batch_identifiers