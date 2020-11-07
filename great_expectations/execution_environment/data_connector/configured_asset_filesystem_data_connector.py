import itertools
from pathlib import Path
from typing import Any, List, Union, Dict, Optional, Iterator
import copy
import logging
import os

from great_expectations.core.batch import BatchDefinition, BatchMarkers, BatchRequest
from great_expectations.core.id_dict import BatchSpec, PartitionDefinitionSubset
from great_expectations.execution_environment.data_connector import ConfiguredAssetFilePathDataConnector
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset import Asset
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
    normalize_directory_path,
    get_filesystem_one_level_directory_glob_path_list,
    build_sorters_from_config,
)
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)

# TODO: <Alex>Should we make this a "set" object?</Alex>
# TODO: <Alex>ALEX Is this needed?</Alex>
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


class ConfiguredAssetFilesystemDataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        base_directory: str,
        default_regex: dict,
        assets: dict,
        glob_directive: str = "*",
        sorters: list = None,
        execution_engine: ExecutionEngine = None,
        data_context_root_directory: str = None,
    ):
        logger.debug(f'Constructing ConfiguredAssetFilesystemDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            assets=assets,
            sorters=sorters,
            default_regex=default_regex,
            data_context_root_directory=data_context_root_directory
        )

        self.base_directory = normalize_directory_path(
            dir_path=base_directory,
            root_directory_path=data_context_root_directory
        )

        self.glob_directive = glob_directive

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        data_asset_path: str = self.base_directory
        if asset is not None and asset.base_directory:
            data_asset_path = str(Path(self.base_directory).joinpath(asset.base_directory))

        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=data_asset_path,
            glob_directive=self.glob_directive
        )

        return path_list

    def _get_full_file_path(self, path: str) -> str:
        return str(Path(self.base_directory).joinpath(path))
