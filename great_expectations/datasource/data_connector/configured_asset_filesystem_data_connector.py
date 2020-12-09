import logging
from pathlib import Path
from typing import List, Optional

from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class ConfiguredAssetFilesystemDataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        assets: dict,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        glob_directive: str = "*",
        sorters: Optional[list] = None,
    ):
        logger.debug(f'Constructing ConfiguredAssetFilesystemDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            assets=assets,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
        )

        self._base_directory = base_directory
        self._glob_directive = glob_directive

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        base_directory: str = self.base_directory
        glob_directive: str = self._glob_directive

        if asset is not None:
            if asset.base_directory:
                base_directory = normalize_directory_path(
                    dir_path=asset.base_directory, root_directory_path=base_directory
                )
            if asset.glob_directive:
                glob_directive = asset.glob_directive

        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=base_directory, glob_directive=glob_directive
        )

        return sorted(path_list)

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        base_directory: str = self.base_directory
        if asset is not None:
            if asset.base_directory:
                base_directory = normalize_directory_path(
                    dir_path=asset.base_directory, root_directory_path=base_directory,
                )
        return str(Path(base_directory).joinpath(path))

    @property
    def base_directory(self) -> str:
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory,
        )
