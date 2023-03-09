import pathlib
import re
import typing
from logging import Logger
from typing import Optional, Sequence, Union

from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api as public_api
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector as FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    Sorter as Sorter,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition as SortersDefinition,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError as TestConnectionError,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset as CSVAsset,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    ExcelAsset as ExcelAsset,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    JSONAsset as JSONAsset,
)
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    ParquetAsset as ParquetAsset,
)

logger: Logger

class PandasFilesystemDatasource(_PandasFilePathDatasource):
    type: Literal["pandas_filesystem"]
    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path]
    def test_connection(self, test_assets: bool = ...) -> None: ...
    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        sep: typing.Any = ...,
        delimiter: typing.Any = None,
        header: typing.Any = "infer",
        names: typing.Any = ...,
        index_col: typing.Any = None,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        squeeze: typing.Any = False,
        prefix: typing.Any = ...,
        mangle_dupe_cols: typing.Any = True,
        dtype: typing.Union[dict, None] = None,
        engine: typing.Any = None,
        converters: typing.Any = None,
        true_values: typing.Any = None,
        false_values: typing.Any = None,
        skipinitialspace: typing.Any = False,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        skipfooter: typing.Any = 0,
        nrows: typing.Any = None,
        na_values: typing.Any = None,
        keep_default_na: typing.Any = True,
        na_filter: typing.Any = True,
        verbose: typing.Any = False,
        skip_blank_lines: typing.Any = True,
        parse_dates: typing.Any = False,
        infer_datetime_format: typing.Any = False,
        keep_date_col: typing.Any = False,
        date_parser: typing.Any = None,
        dayfirst: typing.Any = False,
        cache_dates: typing.Any = True,
        iterator: typing.Any = False,
        chunksize: typing.Any = None,
        compression: typing.Any = "infer",
        thousands: typing.Any = None,
        decimal: str = ".",
        lineterminator: typing.Any = None,
        quotechar: typing.Any = '"',
        quoting: typing.Any = 0,
        doublequote: typing.Any = True,
        escapechar: typing.Any = None,
        comment: typing.Any = None,
        encoding: typing.Any = None,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = None,
        error_bad_lines: typing.Any = None,
        warn_bad_lines: typing.Any = None,
        on_bad_lines: typing.Any = None,
        delim_whitespace: typing.Any = False,
        low_memory: typing.Any = True,
        memory_map: typing.Any = False,
        float_precision: typing.Any = None,
        storage_options: StorageOptions = None,
    ) -> CSVAsset: ...
    def add_excel_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        sheet_name: typing.Any = 0,
        header: typing.Any = 0,
        names: typing.Any = None,
        index_col: typing.Any = None,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        squeeze: typing.Any = False,
        dtype: typing.Union[dict, None] = None,
        engine: typing.Any = None,
        converters: typing.Any = None,
        true_values: typing.Any = None,
        false_values: typing.Any = None,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        nrows: typing.Any = None,
        na_values: typing.Any = None,
        keep_default_na: typing.Any = True,
        na_filter: typing.Any = True,
        verbose: typing.Any = False,
        parse_dates: typing.Any = False,
        date_parser: typing.Any = None,
        thousands: typing.Any = None,
        comment: typing.Any = None,
        skipfooter: typing.Any = 0,
        convert_float: typing.Any = None,
        mangle_dupe_cols: typing.Any = True,
        storage_options: StorageOptions = None,
    ) -> ExcelAsset: ...
    def add_feather_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        columns: typing.Any = None,
        use_threads: bool = True,
        storage_options: typing.Union[typing.Dict[str, typing.Any], None] = None,
    ) -> FeatherAsset: ...
    def add_hdf_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        key: typing.Any = None,
        mode: str = "r",
        errors: str = "strict",
        where: typing.Any = None,
        start: typing.Union[int, None] = None,
        stop: typing.Union[int, None] = None,
        columns: typing.Any = None,
        iterator: typing.Any = False,
        chunksize: typing.Union[int, None] = None,
        kwargs: typing.Union[dict, None] = None,
    ) -> HDFAsset: ...
    def add_html_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        match: Union[str, Pattern] = ".+",
        flavor: typing.Union[str, None] = None,
        header: Union[int, Sequence[int], None] = None,
        index_col: Union[int, Sequence[int], None] = None,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        attrs: typing.Union[typing.Dict[str, str], None] = None,
        parse_dates: bool = False,
        thousands: typing.Union[str, None] = ",",
        encoding: typing.Union[str, None] = None,
        decimal: str = ".",
        converters: typing.Union[typing.Dict, None] = None,
        na_values: typing.Any = None,
        keep_default_na: bool = True,
        displayed_only: bool = True,
    ) -> HTMLAsset: ...
    def add_json_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        orient: typing.Any = None,
        typ: typing.Any = "frame",
        dtype: typing.Union[dict, None] = None,
        convert_axes: typing.Any = None,
        convert_dates: typing.Any = True,
        keep_default_dates: bool = True,
        numpy: bool = False,
        precise_float: bool = False,
        date_unit: typing.Any = None,
        encoding: typing.Any = None,
        encoding_errors: typing.Union[str, None] = "strict",
        lines: bool = False,
        chunksize: typing.Union[int, None] = None,
        compression: CompressionOptions = "infer",
        nrows: typing.Union[int, None] = None,
        storage_options: StorageOptions = None,
    ) -> JSONAsset: ...
    def add_orc_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        columns: typing.Union[typing.List[str], None] = None,
        kwargs: typing.Union[dict, None] = None,
    ) -> ORCAsset: ...
    def add_parquet_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        engine: str = "auto",
        columns: typing.Any = None,
        storage_options: StorageOptions = None,
        use_nullable_dtypes: bool = False,
        kwargs: typing.Union[dict, None] = None,
    ) -> ParquetAsset: ...
    def add_pickle_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        compression: typing.Union[str, typing.Dict[str, typing.Any], None] = "infer",
        storage_options: typing.Union[typing.Dict[str, typing.Any], None] = None,
    ) -> PickleAsset: ...
    def add_sas_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        format: typing.Union[str, None] = None,
        index: Union[Hashable, None] = None,
        encoding: typing.Union[str, None] = None,
        chunksize: typing.Union[int, None] = None,
        iterator: bool = False,
    ) -> SASAsset: ...
    def add_spss_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        convert_categoricals: bool = True,
    ) -> SPSSAsset: ...
    def add_stata_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        convert_dates: bool = True,
        convert_categoricals: bool = True,
        index_col: typing.Union[str, None] = None,
        convert_missing: bool = False,
        preserve_dtypes: bool = True,
        columns: Union[Sequence[str], None] = None,
        order_categoricals: bool = True,
        chunksize: typing.Union[int, None] = None,
        iterator: bool = False,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
    ) -> STATAAsset: ...
    def add_xml_asset(
        self,
        name: str,
        order_by: typing.List[
            great_expectations.datasource.fluent.interfaces.Sorter
        ] = None,
        batching_regex: typing.Pattern = ...,
        xpath: typing.Union[str, None] = "./*",
        namespaces: typing.Union[typing.Dict, None] = None,
        elems_only: typing.Union[bool, None] = False,
        attrs_only: typing.Union[bool, None] = False,
        names: typing.Union[typing.List[str], None] = None,
        encoding: typing.Union[str, None] = "utf-8",
        parser: typing.Union[str, None] = "lxml",
        stylesheet: Union[FilePathOrBuffer, None] = None,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
    ) -> XMLAsset: ...
