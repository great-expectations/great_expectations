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


# TODO: <Alex>Clean up order of arguments.</Alex>
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
    ):
        logger.debug(f'Constructing ConfiguredAssetFilesystemDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            assets=assets,
            sorters=sorters,
            default_regex=default_regex,
        )

        self._base_directory = base_directory
        self._glob_directive = glob_directive

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        base_directory: str = self.base_directory
        glob_directive: str = self._glob_directive

        if asset is not None:
            if asset.base_directory:
                base_directory = normalize_directory_path(
                    dir_path=asset.base_directory,
                    root_directory_path=base_directory
                )
            if asset.glob_directive:
                glob_directive = asset.glob_directive

        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=base_directory,
            glob_directive=glob_directive
        )

        return path_list

    def _get_full_file_path(self, path: str) -> str:
        return str(Path(self.base_directory).joinpath(path))

    @property
    def base_directory(self):
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory
        )
