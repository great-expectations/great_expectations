import datetime
import json
import hashlib
from typing import Union, Any

from great_expectations.core.id_dict import (
    IDDict,
    BatchKwargs,
    BatchSpec,
    PartitionRequest,
    PartitionDefinition
)
from great_expectations.exceptions import InvalidBatchIdError
from great_expectations.types import DictDot
import great_expectations.exceptions as ge_exceptions


class BatchDefinition(DictDot):
    def __init__(
        self,
        execution_environment_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_definition: PartitionDefinition,
    ):
        self._validate_batch_definition(
            execution_environment_name=execution_environment_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_definition=partition_definition,
            # limit=limit,
        )

        assert type(partition_definition) == PartitionDefinition

        self._execution_environment_name = execution_environment_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "execution_environment_name": self._execution_environment_name,
            "data_connector_name": self._data_connector_name,
            "data_asset_name": self.data_asset_name,
            "partition_definition": repr(self._partition_definition),
        }
        return str(doc_fields_dict)

    @staticmethod
    def _validate_batch_definition(
        execution_environment_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_definition: PartitionDefinition,
        # limit: Union[int, None] = None,
    ):
        if execution_environment_name is None:
            raise ValueError("A valid execution_environment must be specified.")
        if execution_environment_name and not isinstance(execution_environment_name, str):
            raise TypeError(
                f'''The type of an execution_environment name must be a string (Python "str").  The type given is
"{str(type(execution_environment_name))}", which is illegal.
            '''
            )
        if data_connector_name is None:
            raise ValueError("A valid data_connector must be specified.")
        if data_connector_name and not isinstance(data_connector_name, str):
            raise TypeError(
                f'''The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                '''
            )
        if data_asset_name is None:
            raise ValueError("A valid data_asset_name must be specified.")
        if data_asset_name and not isinstance(data_asset_name, str):
            raise TypeError(
                f'''The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                '''
            )
        if partition_definition and not isinstance(partition_definition, PartitionDefinition):
            raise TypeError(
                f'''The type of a partition_request must be a PartitionDefinition object.  The type given is
"{str(type(partition_definition))}", which is illegal.
                '''
            )
#         if limit and not isinstance(limit, int):
#             raise ge_exceptions.BatchDefinitionError(
#                 f'''The type of limit must be an integer (Python "int").  The type given is "{str(type(limit))}", which
# is illegal.
#                 '''
#             )

    @property
    def execution_environment_name(self) -> str:
        return self._execution_environment_name

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def partition_definition(self) -> PartitionDefinition:
        return self._partition_definition

    def get_json_dict(self) -> dict:
        return {
            "execution_environment_name" : self.execution_environment_name,
            "data_connector_name" : self.data_connector_name,
            "data_asset_name" : self.data_asset_name,
            "partition_definition" : self.partition_definition,
        }

    @property
    def id(self) -> str:
        return hashlib.md5(
            json.dumps(
                self.get_json_dict(),
                sort_keys=True
            ).encode("utf-8")
        ).hexdigest()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        return self.id == other.id

    def __str__(self):
        return json.dumps(
            self.get_json_dict(),
            indent=2
        )


class BatchRequest(DictDot):
    """
    This class contains all attributes of a batch_request.
    """
    def __init__(
        self,
        execution_environment_name: str = None,
        data_connector_name: str = None,
        data_asset_name: str = None,
        partition_request: dict = None,
        in_memory_dataset: Any = None,
        limit: Union[int, None] = None,
        # TODO: <Alex>Is sampling in the scope of the present release?</Alex>
        sampling: Union[dict, None] = None
    ):
        self._validate_batch_request(
            execution_environment_name=execution_environment_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_request=partition_request,
            limit=limit,
        )

        self._execution_environment_name = execution_environment_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._partition_request = partition_request
        self._in_memory_dataset = in_memory_dataset
        self._limit = limit
        self._sampling = sampling

    @property
    def execution_environment_name(self) -> str:
        return self._execution_environment_name

    @property
    def data_connector_name(self) -> str:
        return self._data_connector_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def partition_request(self) -> dict: #PartitionRequest:
        return self._partition_request

    @property
    def in_memory_dataset(self) -> Any:
        return self._in_memory_dataset

    @property
    def limit(self) -> int:
        return self._limit

    @staticmethod
    def _validate_batch_request(
        execution_environment_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_request: Union[PartitionRequest, dict, None] = None,
        limit: Union[int, None] = None,
    ):
        if execution_environment_name and not isinstance(execution_environment_name, str):
            raise TypeError(
                f'''The type of an execution_environment name must be a string (Python "str").  The type given is
"{str(type(execution_environment_name))}", which is illegal.
            '''
            )
        if data_connector_name and not isinstance(data_connector_name, str):
            raise TypeError(
                f'''The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector_name))}", which is illegal.
                '''
            )
        if data_asset_name and not isinstance(data_asset_name, str):
            raise TypeError(
                f'''The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                '''
            )
        #TODO Abe 20201015: Switch this to PartitionRequest.
        if partition_request and not isinstance(partition_request, dict):
            raise TypeError(
                f'''The type of a partition_request must be a dict object.  The type given is
"{str(type(partition_request))}", which is illegal.
                '''
            )
        if limit and not isinstance(limit, int):
            raise TypeError(
                f'''The type of limit must be an integer (Python "int").  The type given is "{str(type(limit))}", which
is illegal.
                '''
            )

    def get_json_dict(self) -> dict:
        return {
            "execution_environment_name" : self.execution_environment_name,
            "data_connector_name" : self.data_connector_name,
            "data_asset_name" : self.data_asset_name,
            "partition_request" : self.partition_request,
        }

    def __str__(self):
        return json.dumps(
            self.get_json_dict(),
            indent=2
        )

    @property
    def id(self) -> str:
        return hashlib.md5(
            json.dumps(
                self.get_json_dict(),
                sort_keys=True
            ).encode("utf-8")
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
# TODO: <Alex>See also "great_expectations/execution_environment/types/batch_spec.py".</Alex>
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
        self._batch_request = batch_request
        self._batch_definition = batch_definition
        if batch_spec is None:
            batch_spec = BatchSpec()
        self._batch_spec = batch_spec

        if not batch_markers:
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

    def __str__(self):
        json_dict = {
            "data": str(self.data),
            "batch_request": self.batch_request.get_json_dict(),
            "batch_definition": self.batch_definition.get_json_dict(),
            "batch_spec": str(self.batch_spec),
            "batch_markers": str(self.batch_markers),
        }
        return json.dumps(json_dict, indent=2)
