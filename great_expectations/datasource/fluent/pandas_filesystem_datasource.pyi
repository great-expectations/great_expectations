import pathlib
import re
import typing
from logging import Logger
from typing import TYPE_CHECKING, Hashable, Iterable, Optional, Sequence, Union

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

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import Sorter
    from great_expectations.datasource.fluent.dynamic_pandas import (
        CompressionOptions,
        CSVEngine,
        FilePath,
        IndexLabel,
        StorageOptions,
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
        sep: typing.Union[str, None] = None,
        delimiter: typing.Union[str, None] = None,
        header: Union[int, Sequence[int], None, Literal["infer"]] = "infer",
        names: Union[Sequence[Hashable], None] = None,
        index_col: Union[IndexLabel, Literal[False], None] = None,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        squeeze: typing.Union[bool, None] = None,
        prefix: str = None,
        mangle_dupe_cols: bool = ...,
        dtype: typing.Union[dict, None] = None,
        engine: Union[CSVEngine, None] = None,
        converters: typing.Any = None,
        true_values: typing.Any = None,
        false_values: typing.Any = None,
        skipinitialspace: bool = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        skipfooter: int = 0,
        nrows: typing.Union[int, None] = None,
        na_values: typing.Any = None,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        skip_blank_lines: bool = ...,
        parse_dates: typing.Any = None,
        infer_datetime_format: bool = ...,
        keep_date_col: bool = ...,
        date_parser: typing.Any = None,
        dayfirst: bool = ...,
        cache_dates: bool = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = None,
        compression: CompressionOptions = "infer",
        thousands: typing.Union[str, None] = None,
        decimal: str = ".",
        lineterminator: typing.Union[str, None] = None,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: typing.Union[str, None] = None,
        comment: typing.Union[str, None] = None,
        encoding: typing.Union[str, None] = None,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = None,
        error_bad_lines: typing.Union[bool, None] = None,
        warn_bad_lines: typing.Union[bool, None] = None,
        on_bad_lines: typing.Any = None,
        delim_whitespace: bool = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        storage_options: StorageOptions = None,
    ) -> CSVAsset: ...
    def add_excel_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        sheet_name: typing.Union[str, int, None] = 0,
        header: Union[int, Sequence[int], None] = 0,
        names: typing.Union[typing.List[str], None] = None,
        index_col: Union[int, Sequence[int], None] = None,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        squeeze: typing.Union[bool, None] = None,
        dtype: typing.Union[dict, None] = None,
        true_values: Union[Iterable[Hashable], None] = None,
        false_values: Union[Iterable[Hashable], None] = None,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        nrows: typing.Union[int, None] = None,
        na_values: typing.Any = None,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        parse_dates: typing.Union[typing.List, typing.Dict, bool] = ...,
        thousands: typing.Union[str, None] = None,
        decimal: str = ".",
        comment: typing.Union[str, None] = None,
        skipfooter: int = 0,
        convert_float: typing.Union[bool, None] = None,
        mangle_dupe_cols: bool = ...,
        storage_options: StorageOptions = None,
    ) -> ExcelAsset: ...
    def add_feather_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        columns: Union[Sequence[Hashable], None] = None,
        use_threads: bool = ...,
        storage_options: StorageOptions = None,
    ) -> FeatherAsset: ...
    def add_hdf_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        key: typing.Any = None,
        mode: str = "r",
        errors: str = "strict",
        where: typing.Union[str, typing.List, None] = None,
        start: typing.Union[int, None] = None,
        stop: typing.Union[int, None] = None,
        columns: typing.Union[typing.List[str], None] = None,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = None,
        kwargs: typing.Union[dict, None] = None,
    ) -> HDFAsset: ...
    def add_html_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        match: Union[str, typing.Pattern] = ".+",
        flavor: typing.Union[str, None] = None,
        header: Union[int, Sequence[int], None] = None,
        index_col: Union[int, Sequence[int], None] = None,
        skiprows: typing.Union[typing.Sequence[int], int, None] = None,
        attrs: typing.Union[typing.Dict[str, str], None] = None,
        parse_dates: bool = ...,
        thousands: typing.Union[str, None] = ",",
        encoding: typing.Union[str, None] = None,
        decimal: str = ".",
        converters: typing.Union[typing.Dict, None] = None,
        na_values: Union[Iterable[object], None] = None,
        keep_default_na: bool = ...,
        displayed_only: bool = ...,
    ) -> HTMLAsset: ...
    def add_json_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]],
        glob_directive: str,
        order_by: Optional[SortersDefinition],
        orient: typing.Union[str, None] = None,
        dtype: typing.Union[dict, None] = None,
        convert_axes: typing.Any = None,
        convert_dates: typing.Union[bool, typing.List[str]] = ...,
        keep_default_dates: bool = ...,
        numpy: bool = ...,
        precise_float: bool = ...,
        date_unit: typing.Union[str, None] = None,
        encoding: typing.Union[str, None] = None,
        encoding_errors: typing.Union[str, None] = "strict",
        lines: bool = ...,
        chunksize: typing.Union[int, None] = None,
        compression: CompressionOptions = "infer",
        nrows: typing.Union[int, None] = None,
        storage_options: StorageOptions = None,
    ) -> JSONAsset: ...
    def add_orc_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
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
        columns: typing.Union[typing.List[str], None] = None,
        storage_options: StorageOptions = None,
        use_nullable_dtypes: bool = ...,
        kwargs: typing.Union[dict, None] = None,
    ) -> ParquetAsset: ...
    def add_pickle_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
    ) -> PickleAsset: ...
    def add_sas_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        format: typing.Union[str, None] = None,
        index: Union[Hashable, None] = None,
        encoding: typing.Union[str, None] = None,
        chunksize: typing.Union[int, None] = None,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
    ) -> SASAsset: ...
    def add_spss_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = None,
        convert_categoricals: bool = ...,
    ) -> SPSSAsset: ...
    def add_stata_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        convert_dates: bool = ...,
        convert_categoricals: bool = ...,
        index_col: typing.Union[str, None] = None,
        convert_missing: bool = ...,
        preserve_dtypes: bool = ...,
        columns: Union[Sequence[str], None] = None,
        order_categoricals: bool = ...,
        chunksize: typing.Union[int, None] = None,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
    ) -> STATAAsset: ...
    def add_xml_asset(
        self,
        name: str,
        order_by: typing.List[Sorter] = None,
        batching_regex: typing.Pattern = ...,
        xpath: str = "./*",
        namespaces: typing.Union[typing.Dict[str, str], None] = None,
        elems_only: bool = ...,
        attrs_only: bool = ...,
        names: Union[Sequence[str], None] = None,
        dtype: typing.Union[dict, None] = None,
        encoding: typing.Union[str, None] = "utf-8",
        stylesheet: Union[FilePath, None] = None,
        iterparse: typing.Union[typing.Dict[str, typing.List[str]], None] = None,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
    ) -> XMLAsset: ...
