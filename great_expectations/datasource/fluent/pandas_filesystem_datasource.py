from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, ClassVar, Literal, Optional, Type

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.file_path_data_asset import (
        _FilePathDataAsset,
    )

logger = logging.getLogger(__name__)


@public_api
class PandasFilesystemDatasource(_PandasFilePathDatasource):
    """Pandas based Datasource for filesystem based data assets."""

    # class attributes
    data_connector_type: ClassVar[
        Type[FilesystemDataConnector]
    ] = FilesystemDataConnector
    # these fields should not be passed to the execution engine
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[set] = {
        "base_directory",
        "data_context_root_directory",
    }

    # instance attributes
    type: Literal["pandas_filesystem"] = "pandas_filesystem"

    # Filesystem specific attributes
    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasFilesystemDatasource.

        Args:
            test_assets: If assets have been passed to the PandasFilesystemDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if not self.base_directory.exists():
            raise TestConnectionError(
                f"Path: {self.base_directory.resolve()} does not exist."
            )

        if self.assets and test_assets:
            for asset in self.assets:
                asset.test_connection()

    def _build_data_connector(
        self, data_asset: _FilePathDataAsset, glob_directive: str = "**/*", **kwargs
    ) -> None:
        """Builds and attaches the `FilesystemDataConnector` to the asset."""
        if kwargs:
            raise TypeError(
                f"_build_data_connector() got unexpected keyword arguments {list(kwargs.keys())}"
            )
        data_asset._data_connector = self.data_connector_type.build_data_connector(
            datasource_name=self.name,
            data_asset_name=data_asset.name,
            batching_regex=data_asset.batching_regex,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
        )

        # build a more specific `_test_connection_error_message`
        data_asset._test_connection_error_message = (
            self.data_connector_type.build_test_connection_error_message(
                data_asset_name=data_asset.name,
                batching_regex=data_asset.batching_regex,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
