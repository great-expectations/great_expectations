from __future__ import annotations

import logging
import pathlib
import re
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    Union,
)

from typing_extensions import Literal

from great_expectations.experimental.datasources.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.experimental.datasources.filesystem_data_asset import (
    _FilesystemDataAsset,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
    DataAsset,
    TestConnectionError,
)
from great_expectations.experimental.datasources.pandas_datasource import (
    _PandasDatasource,
)
from great_expectations.experimental.datasources.signatures import _merge_signatures

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import BatchSorter

logger = logging.getLogger(__name__)


_FILESYSTEM_READER_METHOD_BLACK_LIST = (
    # "read_csv",
    # "read_json",
    # "read_excel",
    # "read_parquet",
    "read_clipboard",  # not path based
    # "read_feather",
    "read_fwf",  # unhandled type
    "read_gbq",  # not path based
    # "read_hdf",
    # "read_html",
    # "read_orc",
    # "read_pickle",
    # "read_sas",  # invalid json schema
    # "read_spss",
    "read_sql",  # not path based & type-name conflict
    "read_sql_query",  # not path based
    "read_sql_table",  # not path based
    "read_table",  # type-name conflict
    # "read_xml",
)

_FILESYSTEM_ASSET_MODELS = _generate_pandas_data_asset_models(
    _FilesystemDataAsset,
    blacklist=_FILESYSTEM_READER_METHOD_BLACK_LIST,
    use_docstring_from_method=True,
    skip_first_param=True,
)

try:
    # variables only needed for type-hinting
    CSVAsset = _FILESYSTEM_ASSET_MODELS["csv"]
    ExcelAsset = _FILESYSTEM_ASSET_MODELS["excel"]
    JSONAsset = _FILESYSTEM_ASSET_MODELS["json"]
    ORCAsset = _FILESYSTEM_ASSET_MODELS["orc"]
    ParquetAsset = _FILESYSTEM_ASSET_MODELS["parquet"]
except KeyError as key_err:
    logger.info(f"zep - {key_err} asset model could not be generated")
    CSVAsset = _FilesystemDataAsset
    ExcelAsset = _FilesystemDataAsset
    JSONAsset = _FilesystemDataAsset
    ORCAsset = _FilesystemDataAsset
    ParquetAsset = _FilesystemDataAsset


class PandasFilesystemDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(
        _FILESYSTEM_ASSET_MODELS.values()
    )

    # instance attributes
    type: Literal["pandas_filesystem"] = "pandas_filesystem"
    name: str
    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None
    assets: Dict[str, _FilesystemDataAsset] = {}

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasDatasource.

        Args:
            test_assets: If assets have been passed to the PandasDatasource, whether to test them as well.

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

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = CSVAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            glob_directive=glob_directive,
            order_by=order_by_sorters,
            **kwargs,
        )

        return self.add_asset(asset)

    def add_excel_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = ExcelAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            glob_directive=glob_directive,
            order_by=order_by_sorters,
            **kwargs,
        )

        return self.add_asset(asset)

    def add_json_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = JSONAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            glob_directive=glob_directive,
            order_by=order_by_sorters,
            **kwargs,
        )

        return self.add_asset(asset)

    def add_parquet_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        glob_directive: str = "**/*",
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasDatasource" object.

        Args:
            name: The name of the csv asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = ParquetAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            glob_directive=glob_directive,
            order_by=order_by_sorters,
            **kwargs,
        )

        return self.add_asset(asset)

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_json_asset.__signature__ = _merge_signatures(add_json_asset, JSONAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_parquet_asset.__signature__ = _merge_signatures(add_parquet_asset, ParquetAsset, exclude={"type"})  # type: ignore[attr-defined]
