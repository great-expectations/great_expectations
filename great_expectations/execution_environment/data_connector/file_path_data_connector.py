import itertools
from pathlib import Path
from typing import Any, List, Union, Dict, Optional, Iterator
import copy
import logging
import os

from great_expectations.core.batch import BatchDefinition, BatchMarkers, BatchRequest
from great_expectations.core.id_dict import BatchSpec, PartitionDefinitionSubset
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partition_query import (
    PartitionQuery,
    build_partition_query

)
from great_expectations.execution_environment.types import PathBatchSpec
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)
from great_expectations.execution_environment.data_connector.sorter import Sorter
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    map_data_reference_string_to_batch_definition_list_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
    get_filesystem_one_level_directory_glob_path_list,
    build_sorters_from_config,
)
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)

# TODO: <Alex>Should we make this a "set" object?</Alex>
# TODO: <Alex>Is this actually needed?</Alex>
KNOWN_EXTENSIONS = [
    ".csv",
    ".tsv",
    ".parquet",
    ".xls",
    ".xlsx",
    ".json",
    ".csv.gz",
    ".tsv.gz",
    ".feather",
]


class FilePathDataConnector(DataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        default_regex: dict,
        execution_engine: ExecutionEngine = None,
        sorters: list = None,
        # TODO: <Alex>This is needed in InferredAsset too ALEX</Alex>
        data_context_root_directory: str = None,
    ):
        logger.debug(f'Constructing FilePathDataConnector "{name}".')
        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
        )
        self._data_context_root_directory = data_context_root_directory

        # TODO: Maybe make this a typed object?
        # TODO: <Alex></Alex>
        if default_regex is None:
            default_regex = {}
        self._default_regex = default_regex

        self._sorters = build_sorters_from_config(config_list=sorters)
        # TODO: <Alex>ALEX</Alex>
        # super()._validate_sorters_configuration()
        self._validate_sorters_configuration()

    @property
    def sorters(self) -> Optional[dict]:
        return self._sorters

    # TODO: <Alex>Consider factoring this out.  ALEX</Alex>
    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[str]:
        """
        Fetch data_references corresponding to data_asset_name from the cache.
        """
        # TODO: <Alex>There is no reason for the BatchRequest semantics here; this should be replaced with a method that accepts just the required arguments.</Alex>
        # TODO: <Alex>Consider factoring this out.  ALEX Like put this into DataConnector</Alex>
        batch_definition_list = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
            )
        )

        regex_config: dict = self._get_regex_config_for_asset(data_asset_name=data_asset_name)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]

        # TODO: <Alex>Consider factoring this out.  ALEX this can be convenience method</Alex>
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
                    for data_reference_sub_cache in self._data_references_cache.values()
                    for batch_definitions in data_reference_sub_cache.values()
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

        if len(self.sorters) > 0:
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

    def build_batch_spec(
        self,
        batch_definition: BatchDefinition
    ) -> PathBatchSpec:
        batch_spec = super().build_batch_spec(batch_definition=batch_definition)
        return PathBatchSpec(batch_spec)

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
        path = self._get_full_file_path(path=path)
        return {
            "path": path
        }

    def _get_full_file_path(self, path: str) -> str:
        raise NotImplementedError

    # TODO: <Alex>What to do with this?  ALEX</Alex>
    def _validate_batch_request(self, batch_request: BatchRequest):
        super()._validate_batch_request(batch_request)
        if self.sorters is not None and len(self.sorters) > 0:
            regex_config = self._default_regex
            if hasattr(self, "assets") and self.assets is not None:

                if batch_request.data_asset_name is not None and \
                        self.assets is not None and \
                        batch_request.data_asset_name in self.assets:

                    asset: Asset = self.assets[batch_request.data_asset_name]
                    if asset.group_names:
                        regex_config["group_names"] = asset.group_names
            group_names: List[str] = regex_config["group_names"]

            if any([sorter not in group_names for sorter in self.sorters]):
                raise ge_exceptions.DataConnectorError(
                    f'''DataConnector "{self.name}" specifies one or more sort keys that do not appear among the
                           configured group_name.
                           '''
                )
            if len(group_names) < len(self.sorters):
                raise ge_exceptions.DataConnectorError(
                    f'''DataConnector "{self.name}" is configured with {len(group_names)} group names;
                               this is fewer than number of sorters specified, which is {len(self.sorters)}.
                             ''')

    # TODO: <Alex>Opportunity to combine code with other connectors into a utility method.</Alex>
    # TODO: <Alex>ALEX This has to work properly at FilePathDataConnector level and for lower connectors</Alex>
    def _validate_sorters_configuration(self, batch_request=None):
        # Override the default
        if len(self.sorters) > 0:
            regex_config = self._default_regex
            if (
                batch_request is not None and batch_request.data_asset_name is not None
                and self.assets and batch_request.data_asset_name in self.assets
            ):
                asset: Asset = self.assets[batch_request.data_asset_name]
                if asset.group_names:
                    regex_config["group_names"] = asset.group_names
            group_names: List[str] = regex_config["group_names"]
            if any([sorter not in group_names for sorter in self.sorters]):
                raise ge_exceptions.DataConnectorError(
                    f'''ConfiguredAssetFilePathDataConnector "{self.name}" specifies one or more sort keys that do not
appear among the configured group_name.
                    '''
                )
            if len(group_names) < len(self.sorters):
                raise ge_exceptions.DataConnectorError(
                    f'''ConfiguredAssetFilePathDataConnector "{self.name}" is configured with {len(group_names)} group
names; this is fewer than number of sorters specified, which is {len(self.sorters)}.
                    '''
                )
