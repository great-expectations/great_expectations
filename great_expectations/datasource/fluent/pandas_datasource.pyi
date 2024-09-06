import os
import sqlite3
import typing
from logging import Logger
from typing import (
    AbstractSet,
    Any,
    Callable,
    ClassVar,
    Hashable,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
from typing_extensions import TypeAlias

from great_expectations._docs_decorators import (
    deprecated_argument,
)
from great_expectations._docs_decorators import (
    public_api as public_api,
)
from great_expectations.compatibility import pydantic, sqlalchemy
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import ColumnPartitioner
from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice
from great_expectations.datasource.fluent.dynamic_pandas import (
    CompressionOptions,
    CSVEngine,
    FilePath,
    IndexLabel,
    StorageOptions,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    BatchMetadata,
    BatchParameters,
    BatchRequest,
    DataAsset,
    Datasource,
)
from great_expectations.execution_engine import PandasExecutionEngine

_EXCLUDE_TYPES_FROM_JSON: list[Type]

MappingIntStrAny: TypeAlias = Mapping[Union[int, str], Any]
AbstractSetIntStr: TypeAlias = AbstractSet[Union[int, str]]
logger: Logger

class PandasDatasourceError(Exception): ...

class _PandasDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]]

    def _get_reader_method(self) -> str: ...
    @override
    def test_connection(self) -> None: ...
    def batch_parameters_template(self) -> BatchParameters: ...
    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch: ...
    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> list[dict]: ...
    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = ...,
        batch_slice: Optional[BatchSlice] = ...,
        partitioner: Optional[ColumnPartitioner] = ...,
    ) -> BatchRequest: ...
    def add_batch_definition_whole_dataframe(self, name: str) -> BatchDefinition: ...
    @override
    def _validate_batch_request(self, batch_request: BatchRequest) -> None: ...
    @override
    def json(  # noqa: PLR0913
        self,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        by_alias: bool = ...,
        skip_defaults: Union[bool, None] = ...,
        exclude_unset: bool = ...,
        exclude_defaults: bool = ...,
        exclude_none: bool = ...,
        encoder: Union[Callable[[Any], Any], None] = ...,
        models_as_dict: bool = ...,
        **dumps_kwargs: Any,
    ) -> str: ...

class ClipboardAsset(_PandasDataAsset): ...
class CSVAsset(_PandasDataAsset): ...
class ExcelAsset(_PandasDataAsset): ...
class FeatherAsset(_PandasDataAsset): ...
class FWFAsset(_PandasDataAsset): ...
class GBQAsset(_PandasDataAsset): ...
class HDFAsset(_PandasDataAsset): ...
class HTMLAsset(_PandasDataAsset): ...
class JSONAsset(_PandasDataAsset): ...
class ORCAsset(_PandasDataAsset): ...
class ParquetAsset(_PandasDataAsset): ...
class PickleAsset(_PandasDataAsset): ...
class SQLAsset(_PandasDataAsset): ...
class SQLQueryAsset(_PandasDataAsset): ...
class SQLTableAsset(_PandasDataAsset): ...
class SASAsset(_PandasDataAsset): ...
class SPSSAsset(_PandasDataAsset): ...
class StataAsset(_PandasDataAsset): ...
class TableAsset(_PandasDataAsset): ...
class XMLAsset(_PandasDataAsset): ...

class DataFrameAsset(_PandasDataAsset):
    type: Literal["dataframe"]

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = ...,
        batch_slice: Optional[BatchSlice] = ...,
        partitioner: Optional[ColumnPartitioner] = ...,
    ) -> BatchRequest: ...
    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch: ...
    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]: ...

_PandasDataAssetT = TypeVar("_PandasDataAssetT", bound=_PandasDataAsset)

class _PandasDatasource(Datasource):
    asset_types: ClassVar[Sequence[Type[DataAsset]]]
    assets: MutableSequence[_PandasDataAssetT]  # type: ignore[valid-type]
    @property
    @override
    def execution_engine_type(self) -> Type[PandasExecutionEngine]: ...
    @override
    def test_connection(self, test_assets: bool = ...) -> None: ...
    @override
    def json(  # noqa: PLR0913
        self,
        *,
        include: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        exclude: Union[AbstractSetIntStr, MappingIntStrAny, None] = ...,
        by_alias: bool = ...,
        skip_defaults: Union[bool, None] = ...,
        exclude_unset: bool = ...,
        exclude_defaults: bool = ...,
        exclude_none: bool = ...,
        encoder: Union[Callable[[Any], Any], None] = ...,
        models_as_dict: bool = ...,
        **dumps_kwargs: Any,
    ) -> str: ...

_DYNAMIC_ASSET_TYPES: list[Type[_PandasDataAsset]]

class PandasDatasource(_PandasDatasource):
    asset_types: ClassVar[Sequence[Type[DataAsset]]]
    type: Literal["pandas"]
    assets: List[_PandasDataAsset]
    @override
    def test_connection(self, test_assets: bool = ...) -> None: ...
    @deprecated_argument(
        argument_name="dataframe",
        message='The "dataframe" argument is no longer part of "PandasDatasource.add_dataframe_asset()" method call; instead, "dataframe" is the required argument to "DataFrameAsset.build_batch_request()" method.',  # noqa: E501
        version="0.16.15",
    )
    def add_dataframe_asset(
        self,
        name: str,
        *,
        dataframe: Optional[pd.DataFrame] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
    ) -> DataFrameAsset: ...
    def read_dataframe(
        self,
        dataframe: pd.DataFrame,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
    ) -> Batch: ...
    def add_clipboard_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: str = "\\s+",
        kwargs: typing.Union[dict, None] = ...,
    ) -> ClipboardAsset: ...
    def add_csv_asset(  # noqa: PLR0913
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: typing.Union[str, None] = ...,
        delimiter: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None, Literal["infer"]] = "infer",
        names: Union[Sequence[Hashable], None] = ...,
        index_col: Union[IndexLabel, Literal[False], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        prefix: str = ...,
        mangle_dupe_cols: bool = ...,
        dtype: typing.Union[dict, None] = ...,
        engine: Union[CSVEngine, None] = ...,
        converters: typing.Any = ...,
        true_values: typing.Any = ...,
        false_values: typing.Any = ...,
        skipinitialspace: bool = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        skipfooter: int = 0,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        skip_blank_lines: bool = ...,
        parse_dates: typing.Any = ...,
        infer_datetime_format: bool = ...,
        keep_date_col: bool = ...,
        date_parser: typing.Any = ...,
        dayfirst: bool = ...,
        cache_dates: bool = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        lineterminator: typing.Union[str, None] = ...,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: typing.Union[str, None] = ...,
        comment: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = ...,
        error_bad_lines: typing.Union[bool, None] = ...,
        warn_bad_lines: typing.Union[bool, None] = ...,
        on_bad_lines: typing.Any = ...,
        delim_whitespace: bool = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> CSVAsset: ...
    def add_excel_asset(  # noqa: PLR0913
        self,
        name: str,
        io: os.PathLike | str | bytes,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        sheet_name: typing.Union[str, int, None] = 0,
        header: Union[int, Sequence[int], None] = 0,
        names: typing.Union[typing.List[str], None] = ...,
        index_col: Union[int, Sequence[int], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        dtype: typing.Union[dict, None] = ...,
        true_values: Union[Iterable[Hashable], None] = ...,
        false_values: Union[Iterable[Hashable], None] = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        parse_dates: typing.Union[typing.List, typing.Dict, bool] = ...,
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        comment: typing.Union[str, None] = ...,
        skipfooter: int = 0,
        convert_float: typing.Union[bool, None] = ...,
        mangle_dupe_cols: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> ExcelAsset: ...
    def add_feather_asset(  # noqa: PLR0913
        self,
        name: str,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        columns: Union[Sequence[Hashable], None] = ...,
        use_threads: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> FeatherAsset: ...
    def add_fwf_asset(  # noqa: PLR0913
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        colspecs: Union[Sequence[Tuple[int, int]], str, None] = ...,
        widths: Union[Sequence[int], None] = ...,
        infer_nrows: int = ...,
        kwargs: Optional[dict] = ...,
    ) -> FWFAsset: ...
    def add_gbq_asset(  # noqa: PLR0913
        self,
        name: str,
        query: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        project_id: typing.Union[str, None] = ...,
        index_col: typing.Union[str, None] = ...,
        col_order: typing.Union[typing.List[str], None] = ...,
        reauth: bool = ...,
        auth_local_webserver: bool = ...,
        dialect: typing.Union[str, None] = ...,
        location: typing.Union[str, None] = ...,
        configuration: typing.Union[typing.Dict[str, typing.Any], None] = ...,
        credentials: typing.Any = ...,
        use_bqstorage_api: typing.Union[bool, None] = ...,
        max_results: typing.Union[int, None] = ...,
        progress_bar_type: typing.Union[str, None] = ...,
    ) -> GBQAsset: ...
    def add_hdf_asset(  # noqa: PLR0913
        self,
        name: str,
        path_or_buf: str | os.PathLike | pd.HDFStore,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        key: typing.Any = ...,
        mode: str = "r",
        errors: str = "strict",
        where: typing.Union[str, typing.List, None] = ...,
        start: typing.Union[int, None] = ...,
        stop: typing.Union[int, None] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> HDFAsset: ...
    def add_html_asset(  # noqa: PLR0913
        self,
        name: str,
        io: os.PathLike | str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        match: Union[str, typing.Pattern] = ".+",
        flavor: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None] = ...,
        index_col: Union[int, Sequence[int], None] = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        attrs: typing.Union[typing.Dict[str, str], None] = ...,
        parse_dates: bool = ...,
        thousands: typing.Union[str, None] = ",",
        encoding: typing.Union[str, None] = ...,
        decimal: str = ".",
        converters: typing.Union[typing.Dict, None] = ...,
        na_values: Union[Iterable[object], None] = ...,
        keep_default_na: bool = ...,
        displayed_only: bool = ...,
    ) -> HTMLAsset: ...
    def add_json_asset(  # noqa: PLR0913
        self,
        name: str,
        path_or_buf: pydantic.Json | pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        orient: typing.Union[str, None] = ...,
        dtype: typing.Union[dict, None] = ...,
        convert_axes: typing.Any = ...,
        convert_dates: typing.Union[bool, typing.List[str]] = ...,
        keep_default_dates: bool = ...,
        numpy: bool = ...,
        precise_float: bool = ...,
        date_unit: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        lines: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        nrows: typing.Union[int, None] = ...,
        storage_options: StorageOptions = ...,
    ) -> JSONAsset: ...
    def add_orc_asset(
        self,
        name: str,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> ORCAsset: ...
    def add_parquet_asset(  # noqa: PLR0913
        self,
        name: str,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        engine: str = "auto",
        columns: typing.Union[typing.List[str], None] = ...,
        storage_options: StorageOptions = ...,
        use_nullable_dtypes: bool = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> Optional[ParquetAsset]: ...
    def add_pickle_asset(
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> PickleAsset: ...
    def add_sas_asset(  # noqa: PLR0913
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        format: typing.Union[str, None] = ...,
        index: Union[Hashable, None] = ...,
        encoding: typing.Union[str, None] = ...,
        chunksize: typing.Union[int, None] = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
    ) -> SASAsset: ...
    def add_spss_asset(
        self,
        name: str,
        path: pydantic.FilePath,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        convert_categoricals: bool = ...,
    ) -> SPSSAsset: ...
    def add_sql_asset(  # noqa: PLR0913
        self,
        name: str,
        sql: sa.select | sa.text | str,  # type: ignore[valid-type]
        con: sqlalchemy.Engine | sqlite3.Connection | str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        params: typing.Any = ...,
        parse_dates: typing.Any = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
    ) -> SQLAsset: ...
    def add_sql_query_asset(  # noqa: PLR0913
        self,
        name: str,
        sql: sa.select | sa.text | str,  # type: ignore[valid-type]
        con: sqlalchemy.Engine | sqlite3.Connection | str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        params: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        parse_dates: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
        dtype: typing.Union[dict, None] = ...,
    ) -> SQLQueryAsset: ...
    def add_sql_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table_name: str,
        con: sqlalchemy.Engine | str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        schema: typing.Union[str, None] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        parse_dates: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
    ) -> SQLTableAsset: ...
    def add_stata_asset(  # noqa: PLR0913
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        convert_dates: bool = ...,
        convert_categoricals: bool = ...,
        index_col: typing.Union[str, None] = ...,
        convert_missing: bool = ...,
        preserve_dtypes: bool = ...,
        columns: Union[Sequence[str], None] = ...,
        order_categoricals: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> StataAsset: ...
    def add_table_asset(  # noqa: PLR0913
        self,
        name: str,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: typing.Union[str, None] = ...,
        delimiter: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None, Literal["infer"]] = "infer",
        names: Union[Sequence[Hashable], None] = ...,
        index_col: Union[IndexLabel, Literal[False], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        prefix: str = ...,
        mangle_dupe_cols: bool = ...,
        dtype: typing.Union[dict, None] = ...,
        engine: Union[CSVEngine, None] = ...,
        converters: typing.Any = ...,
        true_values: typing.Any = ...,
        false_values: typing.Any = ...,
        skipinitialspace: bool = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        skipfooter: int = 0,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        skip_blank_lines: bool = ...,
        parse_dates: typing.Any = ...,
        infer_datetime_format: bool = ...,
        keep_date_col: bool = ...,
        date_parser: typing.Any = ...,
        dayfirst: bool = ...,
        cache_dates: bool = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        lineterminator: typing.Union[str, None] = ...,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: typing.Union[str, None] = ...,
        comment: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = ...,
        error_bad_lines: typing.Union[bool, None] = ...,
        warn_bad_lines: typing.Union[bool, None] = ...,
        on_bad_lines: typing.Any = ...,
        delim_whitespace: typing.Any = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        float_precision: typing.Union[str, None] = ...,
        storage_options: StorageOptions = ...,
    ) -> TableAsset: ...
    def add_xml_asset(  # noqa: PLR0913
        self,
        name: str,
        path_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        xpath: str = "./*",
        namespaces: typing.Union[typing.Dict[str, str], None] = ...,
        elems_only: bool = ...,
        attrs_only: bool = ...,
        names: Union[Sequence[str], None] = ...,
        dtype: typing.Union[dict, None] = ...,
        encoding: typing.Union[str, None] = "utf-8",
        stylesheet: Union[FilePath, None] = ...,
        iterparse: typing.Union[typing.Dict[str, typing.List[str]], None] = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> XMLAsset: ...
    def read_clipboard(
        self,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: str = r"\s+",
        kwargs: typing.Union[dict, None] = ...,
    ) -> Batch: ...
    def read_csv(  # noqa: PLR0913
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: typing.Union[str, None] = ...,
        delimiter: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None, Literal["infer"]] = "infer",
        names: Union[Sequence[Hashable], None] = ...,
        index_col: Union[IndexLabel, Literal[False], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        prefix: str = ...,
        mangle_dupe_cols: bool = ...,
        dtype: typing.Union[dict, None] = ...,
        engine: Union[CSVEngine, None] = ...,
        converters: typing.Any = ...,
        true_values: typing.Any = ...,
        false_values: typing.Any = ...,
        skipinitialspace: bool = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        skipfooter: int = 0,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        skip_blank_lines: bool = ...,
        parse_dates: typing.Any = ...,
        infer_datetime_format: bool = ...,
        keep_date_col: bool = ...,
        date_parser: typing.Any = ...,
        dayfirst: bool = ...,
        cache_dates: bool = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        lineterminator: typing.Union[str, None] = ...,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: typing.Union[str, None] = ...,
        comment: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = ...,
        error_bad_lines: typing.Union[bool, None] = ...,
        warn_bad_lines: typing.Union[bool, None] = ...,
        on_bad_lines: typing.Any = ...,
        delim_whitespace: bool = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_excel(  # noqa: PLR0913
        self,
        io: os.PathLike | str | bytes,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        sheet_name: typing.Union[str, int, None] = 0,
        header: Union[int, Sequence[int], None] = 0,
        names: typing.Union[typing.List[str], None] = ...,
        index_col: Union[int, Sequence[int], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        dtype: typing.Union[dict, None] = ...,
        true_values: Union[Iterable[Hashable], None] = ...,
        false_values: Union[Iterable[Hashable], None] = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        parse_dates: typing.Union[typing.List, typing.Dict, bool] = ...,
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        comment: typing.Union[str, None] = ...,
        skipfooter: int = 0,
        convert_float: typing.Union[bool, None] = ...,
        mangle_dupe_cols: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_feather(  # noqa: PLR0913
        self,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        columns: Union[Sequence[Hashable], None] = ...,
        use_threads: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_fwf(  # noqa: PLR0913
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        colspecs: Union[Sequence[Tuple[int, int]], str, None] = ...,
        widths: Union[Sequence[int], None] = ...,
        infer_nrows: int = ...,
        kwargs: Optional[dict] = ...,
    ) -> Batch: ...
    def read_gbq(  # noqa: PLR0913
        self,
        query: str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        project_id: typing.Union[str, None] = ...,
        index_col: typing.Union[str, None] = ...,
        col_order: typing.Union[typing.List[str], None] = ...,
        reauth: bool = ...,
        auth_local_webserver: bool = ...,
        dialect: typing.Union[str, None] = ...,
        location: typing.Union[str, None] = ...,
        configuration: typing.Union[typing.Dict[str, typing.Any], None] = ...,
        credentials: typing.Any = ...,
        use_bqstorage_api: typing.Union[bool, None] = ...,
        max_results: typing.Union[int, None] = ...,
        progress_bar_type: typing.Union[str, None] = ...,
    ) -> Batch: ...
    def read_hdf(  # noqa: PLR0913
        self,
        path_or_buf: pd.HDFStore | os.PathLike | str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        key: typing.Any = ...,
        mode: str = "r",
        errors: str = "strict",
        where: typing.Union[str, typing.List, None] = ...,
        start: typing.Union[int, None] = ...,
        stop: typing.Union[int, None] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> Batch: ...
    def read_html(  # noqa: PLR0913
        self,
        io: os.PathLike | str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        match: Union[str, typing.Pattern] = ".+",
        flavor: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None] = ...,
        index_col: Union[int, Sequence[int], None] = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        attrs: typing.Union[typing.Dict[str, str], None] = ...,
        parse_dates: bool = ...,
        thousands: typing.Union[str, None] = ",",
        encoding: typing.Union[str, None] = ...,
        decimal: str = ".",
        converters: typing.Union[typing.Dict, None] = ...,
        na_values: Union[Iterable[object], None] = ...,
        keep_default_na: bool = ...,
        displayed_only: bool = ...,
    ) -> Batch: ...
    def read_json(  # noqa: PLR0913
        self,
        path_or_buf: pydantic.Json | pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        orient: typing.Union[str, None] = ...,
        dtype: typing.Union[dict, None] = ...,
        convert_axes: typing.Any = ...,
        convert_dates: typing.Union[bool, typing.List[str]] = ...,
        keep_default_dates: bool = ...,
        numpy: bool = ...,
        precise_float: bool = ...,
        date_unit: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        lines: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        nrows: typing.Union[int, None] = ...,
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_orc(
        self,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> Batch: ...
    def read_parquet(  # noqa: PLR0913
        self,
        path: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        engine: str = "auto",
        columns: typing.Union[typing.List[str], None] = ...,
        storage_options: StorageOptions = ...,
        use_nullable_dtypes: bool = ...,
        kwargs: typing.Union[dict, None] = ...,
    ) -> Batch: ...
    def read_pickle(
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_sas(  # noqa: PLR0913
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        format: typing.Union[str, None] = ...,
        index: Union[Hashable, None] = ...,
        encoding: typing.Union[str, None] = ...,
        chunksize: typing.Union[int, None] = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
    ) -> Batch: ...
    def read_spss(
        self,
        path: pydantic.FilePath,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        convert_categoricals: bool = ...,
    ) -> Batch: ...
    def read_sql(  # noqa: PLR0913
        self,
        sql: sa.select | sa.text | str,  # type: ignore[valid-type]
        con: sqlalchemy.Engine | sqlite3.Connection | str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        params: typing.Any = ...,
        parse_dates: typing.Any = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
    ) -> Batch: ...
    def read_sql_query(  # noqa: PLR0913
        self,
        sql: sa.select | sa.text | str,  # type: ignore[valid-type]
        con: sqlalchemy.Engine | sqlite3.Connection | str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        params: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        parse_dates: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
        dtype: typing.Union[dict, None] = ...,
    ) -> Batch: ...
    def read_sql_table(  # noqa: PLR0913
        self,
        table_name: str,
        con: sqlalchemy.Engine | str,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        schema: typing.Union[str, None] = ...,
        index_col: typing.Union[str, typing.List[str], None] = ...,
        coerce_float: bool = ...,
        parse_dates: typing.Union[typing.List[str], typing.Dict[str, str], None] = ...,
        columns: typing.Union[typing.List[str], None] = ...,
        chunksize: typing.Union[int, None] = ...,
    ) -> Batch: ...
    def read_stata(  # noqa: PLR0913
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        convert_dates: bool = ...,
        convert_categoricals: bool = ...,
        index_col: typing.Union[str, None] = ...,
        convert_missing: bool = ...,
        preserve_dtypes: bool = ...,
        columns: Union[Sequence[str], None] = ...,
        order_categoricals: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_table(  # noqa: PLR0913
        self,
        filepath_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        sep: typing.Union[str, None] = ...,
        delimiter: typing.Union[str, None] = ...,
        header: Union[int, Sequence[int], None, Literal["infer"]] = "infer",
        names: Union[Sequence[Hashable], None] = ...,
        index_col: Union[IndexLabel, Literal[False], None] = ...,
        usecols: typing.Union[int, str, typing.Sequence[int], None] = ...,
        squeeze: typing.Union[bool, None] = ...,
        prefix: str = ...,
        mangle_dupe_cols: bool = ...,
        dtype: typing.Union[dict, None] = ...,
        engine: Union[CSVEngine, None] = ...,
        converters: typing.Any = ...,
        true_values: typing.Any = ...,
        false_values: typing.Any = ...,
        skipinitialspace: bool = ...,
        skiprows: typing.Union[typing.Sequence[int], int, None] = ...,
        skipfooter: int = 0,
        nrows: typing.Union[int, None] = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        skip_blank_lines: bool = ...,
        parse_dates: typing.Any = ...,
        infer_datetime_format: bool = ...,
        keep_date_col: bool = ...,
        date_parser: typing.Any = ...,
        dayfirst: bool = ...,
        cache_dates: bool = ...,
        iterator: bool = ...,
        chunksize: typing.Union[int, None] = ...,
        compression: CompressionOptions = "infer",
        thousands: typing.Union[str, None] = ...,
        decimal: str = ".",
        lineterminator: typing.Union[str, None] = ...,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: typing.Union[str, None] = ...,
        comment: typing.Union[str, None] = ...,
        encoding: typing.Union[str, None] = ...,
        encoding_errors: typing.Union[str, None] = "strict",
        dialect: typing.Union[str, None] = ...,
        error_bad_lines: typing.Union[bool, None] = ...,
        warn_bad_lines: typing.Union[bool, None] = ...,
        on_bad_lines: typing.Any = ...,
        delim_whitespace: typing.Any = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        float_precision: typing.Union[str, None] = ...,
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
    def read_xml(  # noqa: PLR0913
        self,
        path_or_buffer: pydantic.FilePath | pydantic.AnyUrl,
        *,
        asset_name: Optional[str] = ...,
        batch_metadata: Optional[BatchMetadata] = ...,
        xpath: str = "./*",
        namespaces: typing.Union[typing.Dict[str, str], None] = ...,
        elems_only: bool = ...,
        attrs_only: bool = ...,
        names: Union[Sequence[str], None] = ...,
        dtype: typing.Union[dict, None] = ...,
        encoding: typing.Union[str, None] = "utf-8",
        stylesheet: Union[FilePath, None] = ...,
        iterparse: typing.Union[typing.Dict[str, typing.List[str]], None] = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> Batch: ...
