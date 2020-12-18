import logging
from typing import Any, List, Optional, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinition,
    PartitionDefinitionSubset,
)
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.datasource.types import (
    BatchMarkers,
    BatchSpec,
    RuntimeDataBatchSpec,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


DEFAULT_DATA_ASSET_NAME: str = "IN_MEMORY_DATA_ASSET"
DEFAULT_DELIMITER: str = "-"


# TODO: <Alex>We need a mechanism for specifying the data_asset_name for RuntimeDataConnector (otherwise, it will always be the default).</Alex>
class RuntimeDataConnector(DataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_keys: Optional[list] = None,
    ):
        logger.debug(f'Constructing RuntimeDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
        )

        self._runtime_keys = runtime_keys

    def _refresh_data_references_cache(self):
        """"""
        # Map data_references to batch_definitions
        data_reference: str = self._get_data_reference_list()[0]
        mapped_batch_definition_list: List[
            BatchDefinition
        ] = self._map_data_reference_to_batch_definition_list(
            data_reference=data_reference, data_asset_name=None
        )
        self._data_references_cache = {data_reference: mapped_batch_definition_list}

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        return [""]

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[str]:
        """Fetch data_references corresponding to data_asset_name from the cache."""
        batch_definition_list: List[
            BatchDefinition
        ] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=self.datasource_name,
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
            raise ValueError(
                '_data_references_cache is None.  Have you called "_refresh_data_references_cache()" yet?'
            )

        return [k for k, v in self._data_references_cache.items() if v is None]

    def get_available_data_asset_names(self) -> List[str]:
        # This will fetch ALL batch_definitions in the cache
        batch_definition_list: List[
            BatchDefinition
        ] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
            )
        )

        data_asset_names: set = set()
        for batch_definition in batch_definition_list:
            data_asset_names.add(batch_definition.data_asset_name)

        return list(data_asset_names)

    def get_batch_data_and_metadata(
        self,
        batch_definition: BatchDefinition,
        batch_data: Any,
    ) -> Tuple[Any, BatchSpec, BatchMarkers,]:  # batch_data
        batch_spec: RuntimeDataBatchSpec = self.build_batch_spec(
            batch_definition=batch_definition,
            batch_data=batch_data,
        )
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        self._validate_batch_request(batch_request=batch_request)

        partition_identifiers: Optional[dict] = None
        if batch_request.partition_request:
            self._validate_partition_identifiers(
                partition_identifiers=batch_request.partition_request.get(
                    "partition_identifiers"
                )
            )
            partition_identifiers = batch_request.partition_request.get(
                "partition_identifiers"
            )
        if not partition_identifiers:
            partition_identifiers = {}

        batch_definition_list: List[BatchDefinition]

        batch_definition: BatchDefinition = BatchDefinition(
            datasource_name=self.datasource_name,
            data_connector_name=self.name,
            data_asset_name=DEFAULT_DATA_ASSET_NAME,
            partition_definition=PartitionDefinition(partition_identifiers),
        )

        if batch_definition_matches_batch_request(
            batch_definition=batch_definition, batch_request=batch_request
        ):
            batch_definition_list = [batch_definition]
        else:
            batch_definition_list = []

        return batch_definition_list

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: str, data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        if data_asset_name is None:
            data_asset_name = DEFAULT_DATA_ASSET_NAME
        return [
            BatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
                partition_definition=PartitionDefinition(),
            )
        ]

    def _map_batch_definition_to_data_reference(
        self,
        batch_definition: BatchDefinition,
    ) -> str:
        if not isinstance(batch_definition, BatchDefinition):
            raise TypeError(
                "batch_definition is not of an instance of type BatchDefinition"
            )
        partition_definition: PartitionDefinition = (
            batch_definition.partition_definition
        )
        data_reference: str = self._get_data_reference_name(
            partition_identifiers=partition_definition
        )
        return data_reference

    def _self_check_fetch_batch(
        self,
        pretty_print,
        example_data_reference,
        data_asset_name,
    ):
        return {}

    # This method is currently called called only in tests.
    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        return {}

    # This method is currently called called only in tests.
    def build_batch_spec(
        self,
        batch_definition: BatchDefinition,
        batch_data: Any,
    ) -> RuntimeDataBatchSpec:
        batch_spec = super().build_batch_spec(batch_definition=batch_definition)
        batch_spec["batch_data"] = batch_data
        return RuntimeDataBatchSpec(batch_spec)

    @staticmethod
    def _get_data_reference_name(
        partition_identifiers: PartitionDefinitionSubset,
    ) -> str:
        if partition_identifiers is None:
            partition_identifiers = PartitionDefinitionSubset({})
        data_reference_name = DEFAULT_DELIMITER.join(
            [str(value) for value in partition_identifiers.values()]
        )
        return data_reference_name

    def _validate_batch_request(self, batch_request: BatchRequest):
        super()._validate_batch_request(batch_request=batch_request)

        # Insure that batch_data and batch_request satisfy the "if and only if" condition.
        if not (
            (
                batch_request.batch_data is None
                and (
                    batch_request.partition_request is None
                    or not batch_request.partition_request.get("partition_identifiers")
                )
            )
            or (
                batch_request.batch_data is not None
                and batch_request.partition_request
                and batch_request.partition_request.get("partition_identifiers")
            )
        ):
            raise ge_exceptions.DataConnectorError(
                f"""RuntimeDataConnector "{self.name}" requires batch_data and partition_request to be both present or
                both absent in the batch_request parameter.
                """
            )

    def _validate_partition_identifiers(self, partition_identifiers: dict):
        if partition_identifiers is None:
            partition_identifiers = {}
        self._validate_runtime_keys_configuration(
            runtime_keys=list(partition_identifiers.keys())
        )

    def _validate_runtime_keys_configuration(self, runtime_keys: List[str]):
        if runtime_keys and len(runtime_keys) > 0:
            if not (
                self._runtime_keys and set(runtime_keys) <= set(self._runtime_keys)
            ):
                raise ge_exceptions.DataConnectorError(
                    f"""RuntimeDataConnector "{self.name}" was invoked with one or more runtime keys that do not
appear among the configured runtime keys.
                    """
                )
