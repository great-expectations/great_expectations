from pathlib import Path
from typing import List, Optional
import logging

from great_expectations.execution_environment.data_connector import ConfiguredAssetFilePathDataConnector
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset import Asset
from great_expectations.execution_environment.data_connector.util import (
    normalize_directory_path,
    get_filesystem_one_level_directory_glob_path_list,
)

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
        glob_directive: str = self.glob_directive
        if asset is not None:
            if asset.base_directory:
                data_asset_path = str(Path(self.base_directory).joinpath(asset.base_directory))
            if asset.glob_directive:
                glob_directive = asset.glob_directive

        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=data_asset_path,
            glob_directive=glob_directive
        )

        return path_list

    def _get_full_file_path(self, path: str) -> str:
        return str(Path(self.base_directory).joinpath(path))
