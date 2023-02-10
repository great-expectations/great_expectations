from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Set

from great_expectations.experimental.datasources.data_asset.data_connector.filesystem_data_connector import (
    FilesystemDataConnector,
)
from great_expectations.experimental.datasources.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.experimental.datasources.interfaces import TestConnectionError

if TYPE_CHECKING:
    from great_expectations.experimental.datasources import (
        PandasFilesystemDatasource,
        SparkDatasource,
    )
    from great_expectations.experimental.datasources.data_asset.data_connector.data_connector import (
        DataConnector,
    )

logger = logging.getLogger(__name__)


class _FilesystemDataAsset(_FilePathDataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[
        Set[str]
    ] = _FilePathDataAsset._EXCLUDE_FROM_READER_OPTIONS | {
        "glob_directive",
    }

    # Filesystem specific attributes
    glob_directive: str = "**/*"

    def test_connection(self) -> None:
        """Test the connection for the CSVAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: PandasFilesystemDatasource | SparkDatasource = self.datasource

        success = False
        for filepath in datasource.base_directory.iterdir():
            if self.regex.match(filepath.name):
                # if one file in the path matches the regex, we consider this asset valid
                success = True
                break

        if not success:
            raise TestConnectionError(
                f"No file at path: {datasource.base_directory.resolve()} matched the regex: {self.regex.pattern}"
            )

    def _get_data_connector(self) -> DataConnector:
        data_connector: DataConnector = FilesystemDataConnector(
            name="experimental",
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            execution_engine_name=self.datasource.get_execution_engine().__class__.__name__,
            base_directory=self.datasource.base_directory,
            regex=self.regex,
            glob_directive=self.glob_directive,
        )
        return data_connector

    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for Filesystem DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""
        )

    def _get_reader_options_include(self) -> Set[str] | None:
        raise NotImplementedError(
            """One needs to explicitly provide set(str)-valued reader options for "pydantic.BaseModel.dict()" method \
to use as its "include" directive for Filesystem style DataAsset processing."""
        )
