from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import DBFSPath
from great_expectations.datasource.fluent import PandasFilesystemDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset,
    ExcelAsset,
    JSONAsset,
    ParquetAsset,
)
from great_expectations.datasource.fluent.signatures import _merge_signatures

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import (
        Sorter,
        SortersDefinition,
    )

logger = logging.getLogger(__name__)


@public_api
class PandasDBFSDatasource(PandasFilesystemDatasource):
    """Pandas based Datasource for DataBricks File System (DBFS) based data assets."""

    # instance attributes
    # overridden from base `Literal['pandas_filesystem']`
    type: Literal["pandas_dbfs"] = "pandas_dbfs"  # type: ignore[assignment] # base class has different type

    @public_api
    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsset to the present "PandasDBFSDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches CSV filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[Sorter] or "+/-  key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
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
        asset._data_connector = DBFSDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_file_semantics_version,
        )
        asset._test_connection_error_message = (
            DBFSDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self._add_asset(asset=asset)

    @public_api
    def add_excel_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsset to the present "PandasDBFSDatasource" object.

        Args:
            name: The name of the Excel asset
            batching_regex: regex pattern that matches Excel filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)

        asset = ExcelAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        asset._data_connector = DBFSDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_file_semantics_version,
        )
        asset._test_connection_error_message = (
            DBFSDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self._add_asset(asset=asset)

    @public_api
    def add_json_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsset to the present "PandasDBFSDatasource" object.

        Args:
            name: The name of the JSON asset
            batching_regex: regex pattern that matches JSON filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)

        asset = JSONAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        asset._data_connector = DBFSDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_file_semantics_version,
        )
        asset._test_connection_error_message = (
            DBFSDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self._add_asset(asset=asset)

    @public_api
    def add_parquet_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsset to the present "PandasDBFSDatasource" object.

        Args:
            name: The name of the Parquet asset
            batching_regex: regex pattern that matches Parquet filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)

        asset = ParquetAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        asset._data_connector = DBFSDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_file_semantics_version,
        )
        asset._test_connection_error_message = (
            DBFSDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self._add_asset(asset=asset)

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_json_asset.__signature__ = _merge_signatures(add_json_asset, JSONAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_parquet_asset.__signature__ = _merge_signatures(add_parquet_asset, ParquetAsset, exclude={"type"})  # type: ignore[attr-defined]
