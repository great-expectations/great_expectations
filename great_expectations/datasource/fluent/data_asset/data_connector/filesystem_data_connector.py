from __future__ import annotations

import logging
import pathlib
import re
from typing import List, Optional

from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilePathDataConnector,
)

logger = logging.getLogger(__name__)


class FilesystemDataConnector(FilePathDataConnector):
    """Extension of ConfiguredAssetFilePathDataConnector used to connect to Filesystem.

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        base_directory: Relative path to subdirectory containing files of interest
        glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
        data_context_root_directory: Optional GreatExpectations root directory (if installed on local filesystem)
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): Optional list if you want to sort the data_references
        # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        base_directory: pathlib.Path,
        glob_directive: str = "**/*",
        data_context_root_directory: Optional[pathlib.Path] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
    ) -> None:
        self._base_directory = base_directory
        self._glob_directive: str = glob_directive
        self._data_context_root_directory: Optional[
            pathlib.Path
        ] = data_context_root_directory

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn=None,
        )

    @property
    def base_directory(self) -> pathlib.Path:
        """
        Accessor method for base_directory. If directory is a relative path, interpret it as relative to the
        root directory. If it is absolute, then keep as-is.
        """
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self._data_context_root_directory,
        )

    @classmethod
    def build_data_connector(
        cls,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        base_directory: pathlib.Path,
        glob_directive: str = "**/*",
        data_context_root_directory: Optional[pathlib.Path] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
    ) -> FilesystemDataConnector:
        """Builds "FilesystemDataConnector", which links named DataAsset to local filesystem.

        Args:
            datasource_name: The name of the Datasource associated with this "FilesystemDataConnector" instance
            data_asset_name: The name of the DataAsset using this "FilesystemDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            base_directory: Relative path to subdirectory containing files of interest
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            data_context_root_directory: Optional GreatExpectations root directory (if installed on local filesystem)
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters: optional list of sorters for sorting data_references
            # TODO: <Alex>ALEX</Alex>

        Returns:
            Instantiated "FilesystemDataConnector" object
        """
        return FilesystemDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            base_directory=base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=data_context_root_directory,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
        )

    @classmethod
    def build_test_connection_error_message(
        cls,
        data_asset_name: str,
        batching_regex: re.Pattern,
        base_directory: pathlib.Path,
        glob_directive: str = "**/*",
        data_context_root_directory: Optional[pathlib.Path] = None,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to local filesystem.

        Args:
            data_asset_name: The name of the DataAsset using this "FilesystemDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            base_directory: Relative path to subdirectory containing files of interest
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            data_context_root_directory: Optional GreatExpectations root directory (if installed on local filesystem)

        Returns:
            Customized error message
        """
        test_connection_error_message_template: str = 'No file at base_directory path "{base_directory}" matched regular expressions pattern "{batching_regex}" and/or glob_directive "{glob_directive}" for DataAsset "{data_asset_name}".'
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "batching_regex": batching_regex.pattern,
                "base_directory": base_directory.resolve(),
                "glob_directive": glob_directive,
                "data_context_root_directory": data_context_root_directory,
            }
        )

    # Interface Method
    def get_data_references(self) -> List[str]:
        base_directory: pathlib.Path = self.base_directory
        glob_directive: str = self._glob_directive
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=base_directory, glob_directive=glob_directive
        )
        return sorted(path_list)

    # Interface Method
    def _get_full_file_path(self, path: str) -> str:
        return str(self.base_directory.joinpath(path))
