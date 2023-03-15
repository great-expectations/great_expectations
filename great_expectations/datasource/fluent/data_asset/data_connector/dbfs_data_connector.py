from __future__ import annotations

import logging
import pathlib
import re
from typing import Callable, Optional

from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)

logger = logging.getLogger(__name__)


class DBFSDataConnector(FilesystemDataConnector):
    """Extension of FilePathDataConnector used to connect to the DataBricks File System (DBFS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        base_directory: Relative path to subdirectory containing files of interest
        glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
        data_context_root_directory: Optional GreatExpectations root directory (if installed on DBFS)
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): Optional list if you want to sort the data_references
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on DBFS
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
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        super().__init__(
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
            file_path_template_map_fn=file_path_template_map_fn,
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
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> DBFSDataConnector:
        """Builds "DBFSDataConnector", which links named DataAsset to DBFS.

        Args:
            datasource_name: The name of the Datasource associated with this "DBFSDataConnector" instance
            data_asset_name: The name of the DataAsset using this "DBFSDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            base_directory: Relative path to subdirectory containing files of interest
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            data_context_root_directory: Optional GreatExpectations root directory (if installed on DBFS)
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters: optional list of sorters for sorting data_references
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on DBFS

        Returns:
            Instantiated "DBFSDataConnector" object
        """
        return DBFSDataConnector(
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
            file_path_template_map_fn=file_path_template_map_fn,
        )

    # Interface Method
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""
            )

        template_arguments: dict = {
            "path": str(self.base_directory.joinpath(path)),
        }

        return self._file_path_template_map_fn(**template_arguments)
