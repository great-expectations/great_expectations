import copy
import logging
from typing import List, Optional

from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.datasource.data_connector import FilePathDataConnector
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetFilePathDataConnector(FilePathDataConnector):
    """InferredAssetFilePathDataConnector is a base class for DataConnectors that operate on file paths and determine
    the data_asset_name implicitly (e.g., through the combination of the regular expressions pattern and group names.
    """

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
    ):
        logger.debug(f'Constructing InferredAssetFilePathDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
        )

    def _refresh_data_references_cache(self):
        """
        """
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: List[
                BatchDefinition
            ] = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference, data_asset_name=None
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    def get_data_reference_list_count(self) -> int:
        if self._data_references_cache is None:
            raise ValueError(
                f"data references cache for {self.__class__.__name__} {self.name} has not yet been populated."
            )

        return len(self._data_references_cache)

    def get_unmatched_data_references(self) -> List[str]:
        if self._data_references_cache is None:
            raise ValueError(
                '_data_references_cache is None.  Have you called "_refresh_data_references_cache()" yet?'
            )

        return [k for k, v in self._data_references_cache.items() if v is None]

    def get_available_data_asset_names(self) -> List[str]:
        if self._data_references_cache is None:
            self._refresh_data_references_cache()

        # This will fetch ALL batch_definitions in the cache
        batch_definition_list: List[
            BatchDefinition
        ] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=self.datasource_name, data_connector_name=self.name,
            )
        )

        data_asset_names: List[str] = [
            batch_definition.data_asset_name
            for batch_definition in batch_definition_list
        ]

        return list(set(data_asset_names))

    def _get_batch_definition_list_from_cache(self) -> List[BatchDefinition]:
        batch_definition_list: List[BatchDefinition] = [
            batch_definitions[0]
            for batch_definitions in self._data_references_cache.values()
            if batch_definitions is not None
        ]
        return batch_definition_list

    def _get_regex_config(self, data_asset_name: Optional[str] = None) -> dict:
        regex_config: dict = copy.deepcopy(self._default_regex)
        return regex_config
