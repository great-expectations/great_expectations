from __future__ import annotations

import logging
import pathlib
import re
from typing import ClassVar, Optional, TypeVar, Union

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
    Sorter,
    SortersDefinition,
    TestConnectionError,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset,
)
from great_expectations.datasource.fluent.signatures import _merge_signatures

logger = logging.getLogger(__name__)

_DataAssetT = TypeVar("_DataAssetT", bound=_FilePathDataAsset)


@public_api
class PandasFilesystemDatasource(_PandasFilePathDatasource):
    """Pandas based Datasource for local filesystem based data assets."""

    # class attributes
    _data_connector: ClassVar = FilesystemDataConnector

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

    @public_api
    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasFilesystemDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches CSV filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[Sorter] or "+/-  key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        # TODO: this needs to work for dynamic methods
        # TODO: example my making these validator methods on `DataAsset`
        # or always doing this in the dynamic `add_asset` methods
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = CSVAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )
        asset._data_connector = FilesystemDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
        )
        asset._test_connection_error_message = (
            FilesystemDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self.add_asset(asset=asset)

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

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
