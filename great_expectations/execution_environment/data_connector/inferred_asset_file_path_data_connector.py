from typing import List, Optional, Iterator
import copy

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector import DataConnector
from great_expectations.execution_environment.data_connector import FilePathDataConnector
from great_expectations.execution_environment.data_connector.sorter import Sorter
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
)
from great_expectations.execution_environment.data_connector.partition_query import (
    PartitionQuery,
    build_partition_query,
)
from great_expectations.execution_environment.types import PathBatchSpec
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    map_data_reference_string_to_batch_definition_list_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
    get_filesystem_one_level_directory_glob_path_list,
    build_sorters_from_config,
)
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class InferredAssetFilePathDataConnector(FilePathDataConnector):
    """SinglePartitionerDataConnector is a base class for DataConnectors that require exactly one Partitioner be configured in the declaration.

    Instead, its data_references are stored in a data_reference_dictionary : {
        "pretend/path/A-100.csv" : pandas_df_A_100,
        "pretend/path/A-101.csv" : pandas_df_A_101,
        "pretend/directory/B-1.csv" : pandas_df_B_1,
        "pretend/directory/B-2.csv" : pandas_df_B_2,
        ...
    }
    """

    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine: ExecutionEngine = None,
        default_regex: dict = None,
        sorters: list = None,
        data_context_root_directory: str = None,
    ):
        logger.debug(f'Constructing SinglePartitionerDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            data_context_root_directory=data_context_root_directory
        )

    @property
    def sorters(self) -> Optional[dict]:
        return self._sorters

    def _refresh_data_references_cache(self):
        """
        """
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: List[BatchDefinition] = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference,
                data_asset_name=None
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
            raise ValueError('_data_references_cache is None.  Have you called "_refresh_data_references_cache()" yet?')

        return [k for k, v in self._data_references_cache.items() if v is None]

    def get_available_data_asset_names(self) -> List[str]:
        if self._data_references_cache is None:
            self._refresh_data_references_cache()

        # This will fetch ALL batch_definitions in the cache
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
            )
        )

        data_asset_names: List[str] = [batch_definition.data_asset_name for batch_definition in batch_definition_list]

        return list(set(data_asset_names))

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        self._validate_batch_request(batch_request=batch_request)

        if self._data_references_cache is None:
            self._refresh_data_references_cache()

        batch_definition_list: List[BatchDefinition] = list(
            filter(
                lambda batch_definition: batch_definition_matches_batch_request(
                    batch_definition=batch_definition,
                    batch_request=batch_request
                ),
                [
                    batch_definitions[0]
                    for batch_definitions in self._data_references_cache.values()
                    if batch_definitions is not None
                ]
            )
        )

        # TODO: <Alex>ALEX Can the below be put into a decorator at a higher level?</Alex>
        if batch_request.partition_request is not None:
            partition_query_obj: PartitionQuery = build_partition_query(
                partition_request_dict=batch_request.partition_request
            )
            batch_definition_list = partition_query_obj.select_from_partition_request(
                batch_definition_list=batch_definition_list
            )

        if len(self._sorters) > 0:
            sorted_batch_definition_list = self._sort_batch_definition_list(
                batch_definition_list=batch_definition_list
            )
            return sorted_batch_definition_list
        else:
            return batch_definition_list

    def _sort_batch_definition_list(self, batch_definition_list) -> List[BatchDefinition]:
        sorters: Iterator[Sorter] = reversed(list(self.sorters.values()))
        for sorter in sorters:
            batch_definition_list = sorter.get_sorted_batch_definitions(batch_definitions=batch_definition_list)
        return batch_definition_list

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: str,
        data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        regex_config: dict = copy.deepcopy(self._default_regex)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]
        return map_data_reference_string_to_batch_definition_list_using_regex(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
            data_asset_name=data_asset_name,
            data_reference=data_reference,
            regex_pattern=pattern,
            group_names=group_names
        )

    def _map_batch_definition_to_data_reference(self, batch_definition: BatchDefinition) -> str:
        regex_config: dict = copy.deepcopy(self._default_regex)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]
        return map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=batch_definition,
            regex_pattern=pattern,
            group_names=group_names
        )

    def _get_regex_config(self, data_asset_name: Optional[str] = None) -> dict:
        regex_config: dict = copy.deepcopy(self._default_regex)
        return regex_config
