import copy
import datetime
import hashlib
import json
from typing import Any, Dict, Optional, Union

from great_expectations.core.id_dict import (
    BatchKwargs,
    BatchSpec,
    IDDict,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.exceptions import InvalidBatchIdError
from great_expectations.types import DictDot, SerializableDictDot


class BatchDefinition(SerializableDictDot):
    def __init__(
        self,
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_definition: PartitionDefinition,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        self._validate_batch_definition(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_definition=partition_definition,
            # limit=limit,
        )

        assert type(partition_definition) == PartitionDefinition

        self._datasource_name = datasource_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition
        self._batch_spec_passthrough = batch_spec_passthrough

    def to_json_dict(self) -> Dict:
        return {
            "datasource_name": self._datasource_name,
            "data_connector_name": self._data_connector_name,
            "data_asset_name": self.data_asset_name,
            "partition_definition": self._partition_definition,
        }

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "datasource_name": self._datasource_name,
            "data_connector_name": self._data_connector_name,
            "data_asset_name": self.data_asset_name,
            "partition_definition": repr(self._partition_definition),
        }
        return str(doc_fields_dict)

    @staticmethod
    def _validate_batch_definition(
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_definition: PartitionDefinition,
        # limit: Union[int, None] = None,
    ):
        if datasource_name is None:
            raise ValueError("A valid datasource must be specified.")
        if datasource_name and not isinstance(datasource_name, str):
            raise TypeError(
                f"""The type of an datasource name must be a string (Python "str").  The type given is
"{str(type(datasource_name))}", which is illegal.
            """
            )
        if data_connector_name is None:
            raise ValueError("A valid data_connector must be specified.")
        if data_connector_name and not isinstance(data_connector_name, str):
            raise TypeError(
                f"""The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                """
            )
        if data_asset_name is None:
            raise ValueError("A valid data_asset_name must be specified.")
        if data_asset_name and not isinstance(data_asset_name, str):
            raise TypeError(
                f"""The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                """
            )
        if partition_definition and not isinstance(
            partition_definition, PartitionDefinition
        ):
            raise TypeError(
                f"""The type of a partition_request must be a PartitionDefinition object.  The type given is
"{str(type(partition_definition))}", which is illegal.
                """
            )

    #         if limit and not isinstance(limit, int):
    #             raise ge_exceptions.BatchDefinitionError(
    #                 f'''The type of limit must be an integer (Python "int").  The type given is "{str(type(limit))}", which
    # is illegal.
    #                 '''
    #             )

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def partition_definition(self) -> PartitionDefinition:
        return self._partition_definition

    @property
    def batch_spec_passthrough(self) -> dict:
        return self._batch_spec_passthrough

    @batch_spec_passthrough.setter
    def batch_spec_passthrough(self, batch_spec_passthrough: Optional[dict]):
        self._batch_spec_passthrough = batch_spec_passthrough

    def get_json_dict(self) -> dict:
        return {
            "datasource_name": self.datasource_name,
            "data_connector_name": self.data_connector_name,
            "data_asset_name": self.data_asset_name,
            "partition_definition": self.partition_definition,
        }

    @property
    def id(self) -> str:
        return hashlib.md5(
            json.dumps(self.get_json_dict(), sort_keys=True).encode("utf-8")
        ).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.id == other.id

    def __str__(self):
        return json.dumps(self.get_json_dict(), indent=2)

    def __hash__(self) -> int:
        """Overrides the default implementation"""
        _result_hash: int = (
            hash(self.datasource_name)
            ^ hash(self.data_connector_name)
            ^ hash(self.data_asset_name)
        )
        if self.definition is not None:
            for key, value in self.partition_definition.items():
                _result_hash = _result_hash ^ hash(key) ^ hash(str(value))
        return _result_hash


class BatchRequest(DictDot):
    """
    This class contains all attributes of a batch_request.
    """

    def __init__(
        self,
        datasource_name: str = None,
        data_connector_name: str = None,
        data_asset_name: str = None,
        partition_request: Optional[Union[PartitionRequest, dict]] = None,
        batch_data: Any = None,
        limit: Union[int, None] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        self._validate_batch_request(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_request=partition_request,
            limit=limit,
        )

        self._datasource_name = datasource_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._partition_request = partition_request
        self._batch_data = batch_data
        self._limit = limit
        self._batch_spec_passthrough = batch_spec_passthrough

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def partition_request(self) -> Union[PartitionRequest, dict]:  # PartitionRequest:
        return self._partition_request

    @property
    def batch_data(self) -> Any:
        return self._batch_data

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def batch_spec_passthrough(self) -> dict:
        return self._batch_spec_passthrough

    @staticmethod
    def _validate_batch_request(
        datasource_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_request: Optional[Union[PartitionRequest, dict]] = None,
        limit: Union[int, None] = None,
    ):
        if datasource_name and not isinstance(datasource_name, str):
            raise TypeError(
                f"""The type of an datasource name must be a string (Python "str").  The type given is
"{str(type(datasource_name))}", which is illegal.
            """
            )
        if data_connector_name and not isinstance(data_connector_name, str):
            raise TypeError(
                f"""The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                """
            )
        if data_asset_name and not isinstance(data_asset_name, str):
            raise TypeError(
                f"""The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                """
            )
        # TODO Abe 20201015: Switch this to PartitionRequest.
        if partition_request and not isinstance(partition_request, dict):
            raise TypeError(
                f"""The type of a partition_request must be a dict object.  The type given is
"{str(type(partition_request))}", which is illegal.
                """
            )
        if limit and not isinstance(limit, int):
            raise TypeError(
                f"""The type of limit must be an integer (Python "int").  The type given is "{str(type(limit))}", which
is illegal.
                """
            )

    def get_json_dict(self) -> dict:
        partition_request: Optional[dict] = None
        if self.partition_request is not None:
            partition_request = copy.deepcopy(self.partition_request)
            if partition_request.get("custom_filter_function") is not None:
                partition_request["custom_filter_function"] = partition_request[
                    "custom_filter_function"
                ].__name__
        return {
            "datasource_name": self.datasource_name,
            "data_connector_name": self.data_connector_name,
            "data_asset_name": self.data_asset_name,
            "partition_request": partition_request,
        }

    def __str__(self):
        return json.dumps(self.get_json_dict(), indent=2)

    @property
    def id(self) -> str:
        return hashlib.md5(
            json.dumps(self.get_json_dict(), sort_keys=True).encode("utf-8")
        ).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.id == other.id


# TODO: <Alex>The following class is to support the backward compatibility with the legacy design.</Alex>
class BatchMarkers(BatchKwargs):
    """A BatchMarkers is a special type of BatchKwargs (so that it has a batch_fingerprint) but it generally does
    NOT require specific keys and instead captures information about the OUTPUT of a datasource's fetch
    process, such as the timestamp at which a query was executed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "ge_load_time" not in self:
            raise InvalidBatchIdError("BatchMarkers requires a ge_load_time")

    @property
    def ge_load_time(self):
        return self.get("ge_load_time")


# TODO: <Alex>This module needs to be cleaned up.
#  We have Batch used for the legacy design, and we also need Batch for the new design.
#  However, right now, the Batch from the legacy design is imported into execution engines of the new design.
#  As a result, we have multiple, inconsistent versions of BatchMarkers, extending legacy/new classes.</Alex>
# TODO: <Alex>See also "great_expectations/datasource/types/batch_spec.py".</Alex>
class Batch(DictDot):
    def __init__(
        self,
        data,
        batch_request: BatchRequest = None,
        batch_definition: BatchDefinition = None,
        batch_spec: BatchSpec = None,
        batch_markers: BatchMarkers = None,
        # The remaining parameters are for backward compatibility.
        data_context=None,
        datasource_name=None,
        batch_parameters=None,
        batch_kwargs=None,
    ):
        self._data = data
        if batch_request is None:
            batch_request = dict()
        self._batch_request = batch_request
        if batch_definition is None:
            batch_definition = IDDict()
        self._batch_definition = batch_definition
        if batch_spec is None:
            batch_spec = BatchSpec()
        self._batch_spec = batch_spec

        if batch_markers is None:
            batch_markers = BatchMarkers(
                {
                    "ge_load_time": datetime.datetime.now(
                        datetime.timezone.utc
                    ).strftime("%Y%m%dT%H%M%S.%fZ")
                }
            )
        self._batch_markers = batch_markers

        # The remaining parameters are for backward compatibility.
        self._data_context = data_context
        self._datasource_name = datasource_name
        self._batch_parameters = batch_parameters
        self._batch_kwargs = batch_kwargs or BatchKwargs()

    @property
    def data(self):
        return self._data

    @property
    def batch_request(self):
        return self._batch_request

    @batch_request.setter
    def batch_request(self, batch_request):
        self._batch_request = batch_request

    @property
    def batch_definition(self):
        return self._batch_definition

    @batch_definition.setter
    def batch_definition(self, batch_definition):
        self._batch_definition = batch_definition

    @property
    def batch_spec(self):
        return self._batch_spec

    @property
    def batch_markers(self):
        return self._batch_markers

    # The remaining properties are for backward compatibility.
    @property
    def data_context(self):
        return self._data_context

    @property
    def datasource_name(self):
        return self._datasource_name

    @property
    def batch_parameters(self):
        return self._batch_parameters

    @property
    def batch_kwargs(self):
        return self._batch_kwargs

    @property
    def id(self):
        batch_definition = self._batch_definition
        return (
            batch_definition.id
            if isinstance(batch_definition, BatchDefinition)
            else batch_definition.to_id()
        )

    def __str__(self):
        json_dict = {
            "data": str(self.data),
            "batch_request": self.batch_request.get_json_dict(),
            "batch_definition": self.batch_definition.get_json_dict()
            if isinstance(self.batch_definition, BatchDefinition)
            else {},
            "batch_spec": str(self.batch_spec),
            "batch_markers": str(self.batch_markers),
        }
        return json.dumps(json_dict, indent=2)

    def head(self):
        return self._data.head()
