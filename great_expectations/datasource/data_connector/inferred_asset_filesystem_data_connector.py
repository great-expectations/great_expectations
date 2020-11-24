import logging
from pathlib import Path
from typing import List, Optional

from great_expectations.datasource.data_connector import (
    InferredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetFilesystemDataConnector(InferredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        glob_directive: str = "*",
        sorters: Optional[list] = None,
    ):
        logger.debug(f'Constructing InferredAssetFilesystemDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
        )

        self._base_directory = base_directory
        self._glob_directive = glob_directive

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=self.base_directory, glob_directive=self._glob_directive
        )
        return sorted(path_list)

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None
    ) -> str:
        return str(Path(self.base_directory).joinpath(path))

    @property
    def base_directory(self):
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory,
        )
