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

class BatchRequest(DictDot):
    """
    This class contains all attributes of a batch_request with the exclusion of the in_memory_dataset reference.  This
    is due to the fact that the actual data is not part of the metadata (according to the definition of metadata).
    """
    def __init__(
        self,
        execution_environment: str,
        data_connector: str,
        data_asset_name: str,
        partition_request: Union[dict, PartitionRequest, None] = None,
        limit: Union[int, None] = None,
        # TODO: <Alex>Is sampling in the scope of the present release?</Alex>
        sampling: Union[dict, None] = None
    ):
        if partition_request and isinstance(partition_request, dict):
            partition_request = PartitionRequest(partition_request)
        self._validate_batch_request(
            execution_environment=execution_environment,
            data_connector=data_connector,
            data_asset_name=data_asset_name,
            partition_request=partition_request,
            limit=limit,
        )

        self._execution_environment_name = execution_environment
        self._data_connector_name = data_connector
        self._data_asset_name = data_asset_name
        self._partition_request = partition_request
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
    def partition_request(self) -> PartitionRequest:
        return self._partition_request

    @property
    def limit(self) -> int:
        return self._limit

    @staticmethod
    def _validate_batch_request(
        execution_environment: str,
        data_connector: str,
        data_asset_name: str,
        partition_request: Union[PartitionRequest, None] = None,
        limit: Union[int, None] = None,
    ):
        if execution_environment is None:
            raise ge_exceptions.BatchDefinitionError("A valid execution_environment must be specified.")
        if execution_environment and not isinstance(execution_environment, str):
            raise ge_exceptions.BatchDefinitionError(
                f'''The type of an execution_environment name must be a string (Python "str").  The type given is
"{str(type(execution_environment))}", which is illegal.
            '''
            )
        if data_connector is None:
            raise ge_exceptions.BatchDefinitionError("A valid data_connector must be specified.")
        if data_connector and not isinstance(data_connector, str):
            raise ge_exceptions.BatchDefinitionError(
                f'''The type of a data_connector name must be a string (Python "str").  The type given is
"{str(type(data_connector))}", which is illegal.
                '''
            )
        if data_asset_name is None:
            raise ge_exceptions.BatchDefinitionError("A valid data_asset_name must be specified.")
        if data_asset_name and not isinstance(data_asset_name, str):
            raise ge_exceptions.BatchDefinitionError(
                f'''The type of a data_asset name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
                '''
            )
        if partition_request and not isinstance(partition_request, PartitionRequest):
            raise ge_exceptions.BatchDefinitionError(
                f'''The type of a partition_request must be a PartitionRequest object.  The type given is
"{str(type(partition_request))}", which is illegal.
                '''
            )
        if limit and not isinstance(limit, int):
            raise ge_exceptions.BatchDefinitionError(
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

class BatchDefinition(DictDot):
    def __init__(
        self,
        execution_environment_name: str,
        data_connector_name: str,
        data_asset_name: str,
        partition_definition: PartitionDefinition = None,
    ):
        self._execution_environment_name = execution_environment_name
        self._data_connector_name = data_connector_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition

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
        batch_request: BatchRequest=None,
        batch_definition: BatchDefinition=None,
        batch_spec: BatchSpec=None,
        batch_markers: BatchMarkers=None,
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
