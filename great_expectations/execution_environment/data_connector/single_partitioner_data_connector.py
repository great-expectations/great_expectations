from typing import List, Optional, Iterator
import copy

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
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


class SinglePartitionerDataConnector(DataConnector):
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
        base_directory: str = None,
        glob_directive: str = "*",
        sorters: list = None,
    ):
        logger.debug(f'Constructing SinglePartitionerDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
        )
        self.base_directory = base_directory
        self.glob_directive = glob_directive
        if default_regex is None:
            default_regex = {}
        self._default_regex = default_regex

        self._sorters = build_sorters_from_config(config_list=sorters)
        super()._validate_sorters_configuration()

    @property
    def sorters(self) -> Optional[dict]:
        return self._sorters

    def refresh_data_references_cache(self):
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

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[str]:
        """Fetch data_references corresponding to data_asset_name from the cache.
        """
        # TODO: <Alex>There is no reason for the BatchRequest semantics here; this should be replaced with a method that accepts just the required arguments.</Alex>
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
            )
        )

        regex_config: dict = copy.deepcopy(self._default_regex)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]

        path_list: List[str] = [
            map_batch_definition_to_data_reference_string_using_regex(
                batch_definition=batch_definition,
                regex_pattern=pattern,
                group_names=group_names
            )
            for batch_definition in batch_definition_list
        ]

        # TODO: Sort with a real sorter here
        path_list.sort()

        return path_list

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

        data_asset_names: List[str] = [batch_definition.data_asset_name for batch_definition in batch_definition_list]

        return list(set(data_asset_names))

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        super()._validate_batch_request(batch_request=batch_request)

        if self._data_references_cache is None:
            self.refresh_data_references_cache()

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

        if batch_request.partition_request is not None:
            partition_query_obj: PartitionQuery = build_partition_query(
                partition_request_dict=batch_request.partition_request
            )
            batch_definition_list = partition_query_obj.select_from_partition_request(
                batch_definition_list=batch_definition_list
            )

        if len(self._sorters) > 0:
            sorted_batch_definition_list = self._sort_batch_definition_list(batch_definition_list)
            return sorted_batch_definition_list
        else:
            return batch_definition_list

    def _sort_batch_definition_list(self, batch_definition_list):
        sorters_list = []
        for sorter in self._sorters.values():
            sorters_list.append(sorter)
        sorters: Iterator[Sorter] = reversed(sorters_list)
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

    # TODO: <Alex>This method should be implemented in every subclass.</Alex>
    # def _generate_batch_spec_parameters_from_batch_definition(
    #     self,
    #     batch_definition: BatchDefinition
    # ) -> dict:
    #     pass


class SinglePartitionerFileDataConnector(SinglePartitionerDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        base_directory: str = None,
        default_regex: dict = None,
        glob_directive: str = "*",
        execution_engine: ExecutionEngine = None,
        sorters: List[dict] = None,
    ):
        logger.debug(f'Constructing SinglePartitionerFileDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            base_directory=base_directory,
            glob_directive=glob_directive,
            default_regex=default_regex,
            sorters=sorters,
        )

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=self.base_directory,
            glob_directive=self.glob_directive
        )
        return path_list

    def _generate_batch_spec_parameters_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> dict:
        path: str = self._map_batch_definition_to_data_reference(batch_definition=batch_definition)
        if not path:
            raise ValueError(
                f'''No data reference for data asset name "{batch_definition.data_asset_name}" matches the given
partition definition {batch_definition.partition_definition} from batch definition {batch_definition}.
                '''
            )
        return {
            "path": path
        }

    def _build_batch_spec_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> PathBatchSpec:
        batch_spec = super()._build_batch_spec_from_batch_definition(batch_definition=batch_definition)
        return PathBatchSpec(batch_spec)
