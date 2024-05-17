from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, Callable, Optional

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_connector import (
    FilesystemDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr

logger = logging.getLogger(__name__)


class DBFSDataConnector(FilesystemDataConnector):
    """Extension of FilePathDataConnector used to connect to the DataBricks File System (DBFS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        base_directory: Relative path to subdirectory containing files of interest
        glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
        data_context_root_directory: Optional GreatExpectations root directory (if installed on DBFS)
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on DBFS
        get_unfiltered_batch_definition_list_fn: Function used to get the batch definition list before filtering
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        base_directory: pathlib.Path,
        glob_directive: str = "**/*",
        data_context_root_directory: Optional[pathlib.Path] = None,
        file_path_template_map_fn: Optional[Callable] = None,
        whole_directory_path_override: PathStr | None = None,
    ) -> None:
        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            base_directory=base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=data_context_root_directory,
            file_path_template_map_fn=file_path_template_map_fn,
            whole_directory_path_override=whole_directory_path_override,
        )

    @classmethod
    @override
    def build_data_connector(  # noqa: PLR0913
        cls,
        datasource_name: str,
        data_asset_name: str,
        base_directory: pathlib.Path,
        glob_directive: str = "**/*",
        data_context_root_directory: Optional[pathlib.Path] = None,
        file_path_template_map_fn: Optional[Callable] = None,
        whole_directory_path_override: PathStr | None = None,
    ) -> DBFSDataConnector:
        """Builds "DBFSDataConnector", which links named DataAsset to DBFS.

        Args:
            datasource_name: The name of the Datasource associated with this "DBFSDataConnector" instance
            data_asset_name: The name of the DataAsset using this "DBFSDataConnector" instance
            base_directory: Relative path to subdirectory containing files of interest
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            data_context_root_directory: Optional GreatExpectations root directory (if installed on DBFS)
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on DBFS
            get_unfiltered_batch_definition_list_fn: Function used to get the batch definition list before filtering

        Returns:
            Instantiated "DBFSDataConnector" object
        """  # noqa: E501
        return DBFSDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            base_directory=base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=data_context_root_directory,
            file_path_template_map_fn=file_path_template_map_fn,
            whole_directory_path_override=whole_directory_path_override,
        )

    # Interface Method
    @override
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(  # noqa: TRY003
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""  # noqa: E501
            )

        template_arguments: dict = {
            "path": str(self.base_directory.joinpath(path)),
        }

        return self._file_path_template_map_fn(**template_arguments)
