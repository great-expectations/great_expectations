import logging
from typing import List, Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.experimental.data_asset.data_connector.file_path_data_connector import (
    FilePathDataConnector,
)
from great_expectations.experimental.data_asset.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)

logger = logging.getLogger(__name__)


@public_api
class FilesystemDataConnector(FilePathDataConnector):
    """Extension of ConfiguredAssetFilePathDataConnector used to connect to Filesystem.

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        name (str): name of ConfiguredAssetFilesystemDataConnector
        default_regex (dict): Optional dict the filter and organize the data_references.
        glob_directive (str): glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
        sorters (list): Optional list if you want to sort the data_references
        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
    """

    def __init__(
        self,
        name: str,
        base_directory: str,
        default_regex: Optional[dict] = None,
        glob_directive: str = "**/*",
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ) -> None:
        super().__init__(
            name=name,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        self._base_directory = base_directory
        self._glob_directive = glob_directive

    @property
    def base_directory(self) -> str:
        """
        Accessor method for base_directory. If directory is a relative path, interpret it as relative to the
        root directory. If it is absolute, then keep as-is.
        """
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory,
        )

    def _get_data_reference_list(self) -> List[str]:
        base_directory: str = self.base_directory
        glob_directive: str = self._glob_directive
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=base_directory, glob_directive=glob_directive
        )
        return sorted(path_list)
