from __future__ import annotations

import logging
import pathlib
from typing import ClassVar, Optional, Type, TypeVar

from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)

logger = logging.getLogger(__name__)

_DataAssetT = TypeVar("_DataAssetT", bound=_FilePathDataAsset)


@public_api
class PandasFilesystemDatasource(_PandasFilePathDatasource):
    """Pandas based Datasource for filesystem based data assets."""

    # class attributes
    _data_connector: ClassVar[Type] = FilesystemDataConnector

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
            for asset in self.assets.values():
                asset.test_connection()

    def add_asset(self, asset: _DataAssetT) -> _DataAssetT:
        """
        NOTE: Method 1: override `add_asset` if the Datasource needs to use a data_connector.
        All dynamic `add_<ASSET_TYPE>_asset()` eventually call `add_asset()`.
        """
        # TODO: how do we pull in glob_directive?
        # `add_asset()` accepts an optional `dc_options: dict`?
        # `add_<ASSET_TYPE>_asset()` introspects the DataAsset and DataConnector `__init__` to determine
        # what goes where while still preserving our good validation errors.
        # TODO: or don't be clever and make it a normal asset attribute like `batching_regex`
        glob_directive: str = "**/*"

        asset._data_connector = FilesystemDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=asset.name,
            batching_regex=asset.batching_regex,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
        )
        asset._test_connection_error_message = (
            FilesystemDataConnector.build_test_connection_error_message(
                data_asset_name=asset.name,
                batching_regex=asset.batching_regex,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return super().add_asset(asset)
