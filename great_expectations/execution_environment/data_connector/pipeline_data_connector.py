from typing import List, Any, Optional

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.execution_environment.types import InMemoryBatchSpec
from great_expectations.core.batch import BatchDefinition
from great_expectations.execution_environment.data_connector.util import batch_definition_matches_batch_request
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


DEFAULT_DATA_ASSET_NAME: str = "IN_MEMORY_DATA_ASSET"
DEFAULT_DELIMITER: str = "-"


class PipelineDataConnector(DataConnector):

    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine: ExecutionEngine = None,
        data_asset_name: str = None,
        batch_data: Any = None,
        runtime_keys: dict = None,
        partition_definition: PartitionDefinition = None,
    ):
        logger.debug(f'Constructing PipelineDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
        )

        self._data_asset_name = data_asset_name
        self._batch_data = batch_data
        self._runtime_keys = runtime_keys

        self.partition_definition = partition_definition

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @data_asset_name.setter
    def data_asset_name(self, data_asset_name: str):
        self._data_asset_name = data_asset_name

    @property
    def batch_data(self) -> Any:
        return self._batch_data

    @batch_data.setter
    def batch_data(self, batch_data: Any):
        self._batch_data = batch_data

    @property
    def runtime_keys(self) -> dict:
        return self._runtime_keys

    @runtime_keys.setter
    def runtime_keys(self, runtime_keys: dict):
        self._runtime_keys = runtime_keys

    @property
    def partition_definition(self) -> PartitionDefinition:
        return self._partition_definition

    @partition_definition.setter
    def partition_definition(self, partition_definition: PartitionDefinition):
        if partition_definition:
            self._validate_runtime_keys_configuration(runtime_keys=list(partition_definition.keys()))
        self._partition_definition = partition_definition

    def refresh_data_references_cache(self):
        """
        """
        # Map data_references to batch_definitions
        data_reference: str = self._get_data_reference_list()[0]
        batch_definition_list: List[BatchDefinition] = self._build_batch_definition_list(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
        )
        self._data_references_cache = {
            data_reference: batch_definition_list
        }

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        return [
            self._get_data_reference_name(
                partition_definition=self._partition_definition
            )
        ]

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[str]:
        """Fetch data_references corresponding to data_asset_name from the cache.
        """
        # TODO: <Alex>There is no reason for the BatchRequest semantics here; this should be replaced with a method that accepts just the requirement arguments.</Alex>
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
            )
        )

        if len(batch_definition_list) == 0:
            return []
        return [
            self._map_batch_definition_to_data_reference(
                batch_definition=batch_definition,
            )
            for batch_definition in batch_definition_list
        ]

    def get_data_reference_list_count(self) -> int:
        return len(self._data_references_cache)

    def get_unmatched_data_references(self) -> List[str]:
        if self._data_references_cache is None:
            raise ValueError('_data_references_cache is None.  Have you called "refresh_data_references_cache()" yet?')

        return [k for k, v in self._data_references_cache.items() if v is None]

    def get_available_data_asset_names(self) -> List[str]:
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        # This will fetch ALL batch_definitions in the cache
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
            )
        )

        data_asset_names: set = set()
        for batch_definition in batch_definition_list:
            data_asset_names.add(batch_definition.data_asset_name)

        return list(data_asset_names)

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        self._validate_batch_request(batch_request=batch_request)

        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        batch_definition_list: List[BatchDefinition] = list(self._data_references_cache.values())[0]
        if not batch_definition_matches_batch_request(
            batch_definition=batch_definition_list[0],
            batch_request=batch_request
        ):
            batch_definition_list = []

        return batch_definition_list

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: str,
        data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        return self._build_batch_definition_list(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
        )

    def _map_batch_definition_to_data_reference(
        self,
        batch_definition: BatchDefinition,
    ) -> str:
        if not isinstance(batch_definition, BatchDefinition):
            raise TypeError("batch_definition is not of an instance of type BatchDefinition")
        partition_definition: PartitionDefinition = batch_definition.partition_definition
        data_reference: str = self._get_data_reference_name(
            partition_definition=partition_definition
        )
        return data_reference

    def _generate_batch_spec_parameters_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> dict:
        if self._batch_data is None:
            raise ValueError(
                f'''No partition for data asset name "{batch_definition.data_asset_name}" matches the given partition \
definition {batch_definition.partition_definition} from batch definition {batch_definition}.
                '''
            )
        return {
            "batch_data": self._batch_data
        }

    def _build_batch_definition_list(
        self,
        execution_environment_name: str,
        data_connector_name: str,
    ) -> Optional[List[BatchDefinition]]:
        data_asset_name: str = DEFAULT_DATA_ASSET_NAME
        if self._data_asset_name is not None:
            data_asset_name = self._data_asset_name
        return [
            BatchDefinition(
                execution_environment_name=execution_environment_name,
                data_connector_name=data_connector_name,
                data_asset_name=data_asset_name,
                partition_definition=self._partition_definition
            )
        ]

    def _build_batch_spec_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> InMemoryBatchSpec:
        batch_spec = super()._build_batch_spec_from_batch_definition(batch_definition=batch_definition)
        return InMemoryBatchSpec(batch_spec)
        
    @staticmethod
    def _get_data_reference_name(
        partition_definition: PartitionDefinition
    ) -> str:
        data_reference_name = DEFAULT_DELIMITER.join(
            [
                str(value) for value in partition_definition.values()
            ]
        )
        return data_reference_name

    def _validate_runtime_keys_configuration(self, runtime_keys: List[str]):
        if runtime_keys and len(runtime_keys) > 0:
            if not (self.runtime_keys and set(runtime_keys) <= set(self.runtime_keys)):
                raise ge_exceptions.DataConnectorError(
                    f'''PipelineDataConnector "{self.name}" was invoked with one or more runtime keys that do not \
appear among the configured runtime keys.
                    '''
                )
