from __future__ import annotations

import re
import typing
from logging import Logger
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    Iterable,
    Sequence,
)

from botocore.client import BaseClient as BaseClient
from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api as public_api
from great_expectations.core.util import S3Url as S3Url
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector as FilesystemDataConnector,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector as S3DataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition as SortersDefinition,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError as TestConnectionError,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    PandasDatasourceError as PandasDatasourceError,
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
    from great_expectations.datasource.fluent.config_str import ConfigStr
    from great_expectations.datasource.fluent.dynamic_pandas import (
        CompressionOptions,
        CSVEngine,
        FilePath,
        IndexLabel,
        StorageOptions,
    )
    from great_expectations.datasource.fluent.interfaces import BatchMetadata
    from great_expectations.datasource.fluent.pandas_file_path_datasource import (
        CSVAsset,
        ExcelAsset,
        FeatherAsset,
        HDFAsset,
        HTMLAsset,
        JSONAsset,
        ORCAsset,
        ParquetAsset,
        PickleAsset,
        SASAsset,
        SPSSAsset,
        StataAsset,
        XMLAsset,
    )

logger: Logger
BOTO3_IMPORTED: bool

class PandasS3DatasourceError(PandasDatasourceError): ...

class PandasS3Datasource(_PandasFilePathDatasource):
    type: Literal["pandas_s3"]
    bucket: str
    boto3_options: Dict[str, ConfigStr | Any]
    def test_connection(self, test_assets: bool = ...) -> None: ...
    def add_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        sep: str | None = ...,
        delimiter: str | None = ...,
        header: int | Sequence[int] | None | Literal["infer"] = "infer",
        names: Sequence[Hashable] | None = ...,
        index_col: IndexLabel | Literal[False] | None = ...,
        usecols: int | str | typing.Sequence[int] | None = ...,
        squeeze: bool | None = ...,
        prefix: str = ...,
        mangle_dupe_cols: bool = ...,
        dtype: dict | None = ...,
        engine: CSVEngine | None = ...,
        converters: typing.Any = ...,
        true_values: typing.Any = ...,
        false_values: typing.Any = ...,
        skipinitialspace: bool = ...,
        skiprows: typing.Sequence[int] | int | None = ...,
        skipfooter: int = 0,
        nrows: int | None = ...,
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
        chunksize: int | None = ...,
        compression: CompressionOptions = "infer",
        thousands: str | None = ...,
        decimal: str = ".",
        lineterminator: str | None = ...,
        quotechar: str = '"',
        quoting: int = 0,
        doublequote: bool = ...,
        escapechar: str | None = ...,
        comment: str | None = ...,
        encoding: str | None = ...,
        encoding_errors: str | None = "strict",
        dialect: str | None = ...,
        error_bad_lines: bool | None = ...,
        warn_bad_lines: bool | None = ...,
        on_bad_lines: typing.Any = ...,
        delim_whitespace: bool = ...,
        low_memory: typing.Any = ...,
        memory_map: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> CSVAsset: ...
    def add_excel_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        sheet_name: str | int | None = 0,
        header: int | Sequence[int] | None = 0,
        names: typing.List[str] | None = ...,
        index_col: int | Sequence[int] | None = ...,
        usecols: int | str | typing.Sequence[int] | None = ...,
        squeeze: bool | None = ...,
        dtype: dict | None = ...,
        true_values: Iterable[Hashable] | None = ...,
        false_values: Iterable[Hashable] | None = ...,
        skiprows: typing.Sequence[int] | int | None = ...,
        nrows: int | None = ...,
        na_values: typing.Any = ...,
        keep_default_na: bool = ...,
        na_filter: bool = ...,
        verbose: bool = ...,
        parse_dates: typing.List | typing.Dict | bool = ...,
        thousands: str | None = ...,
        decimal: str = ".",
        comment: str | None = ...,
        skipfooter: int = 0,
        convert_float: bool | None = ...,
        mangle_dupe_cols: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> ExcelAsset: ...
    def add_feather_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        columns: Sequence[Hashable] | None = ...,
        use_threads: bool = ...,
        storage_options: StorageOptions = ...,
    ) -> FeatherAsset: ...
    def add_hdf_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        key: typing.Any = ...,
        mode: str = "r",
        errors: str = "strict",
        where: str | typing.List | None = ...,
        start: int | None = ...,
        stop: int | None = ...,
        columns: typing.List[str] | None = ...,
        iterator: bool = ...,
        chunksize: int | None = ...,
        kwargs: dict | None = ...,
    ) -> HDFAsset: ...
    def add_html_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        match: str | typing.Pattern = ".+",
        flavor: str | None = ...,
        header: int | Sequence[int] | None = ...,
        index_col: int | Sequence[int] | None = ...,
        skiprows: typing.Sequence[int] | int | None = ...,
        attrs: typing.Dict[str, str] | None = ...,
        parse_dates: bool = ...,
        thousands: str | None = ",",
        encoding: str | None = ...,
        decimal: str = ".",
        converters: typing.Dict | None = ...,
        na_values: Iterable[object] | None = ...,
        keep_default_na: bool = ...,
        displayed_only: bool = ...,
    ) -> HTMLAsset: ...
    def add_json_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        orient: str | None = ...,
        dtype: dict | None = ...,
        convert_axes: typing.Any = ...,
        convert_dates: bool | typing.List[str] = ...,
        keep_default_dates: bool = ...,
        numpy: bool = ...,
        precise_float: bool = ...,
        date_unit: str | None = ...,
        encoding: str | None = ...,
        encoding_errors: str | None = "strict",
        lines: bool = ...,
        chunksize: int | None = ...,
        compression: CompressionOptions = "infer",
        nrows: int | None = ...,
        storage_options: StorageOptions = ...,
    ) -> JSONAsset: ...
    def add_orc_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        columns: typing.List[str] | None = ...,
        kwargs: dict | None = ...,
    ) -> ORCAsset: ...
    def add_parquet_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        engine: str = "auto",
        columns: typing.List[str] | None = ...,
        storage_options: StorageOptions = ...,
        use_nullable_dtypes: bool = ...,
        kwargs: dict | None = ...,
    ) -> ParquetAsset: ...
    def add_pickle_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> PickleAsset: ...
    def add_sas_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        format: str | None = ...,
        index: Hashable | None = ...,
        encoding: str | None = ...,
        chunksize: int | None = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
    ) -> SASAsset: ...
    def add_spss_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        usecols: int | str | typing.Sequence[int] | None = ...,
        convert_categoricals: bool = ...,
    ) -> SPSSAsset: ...
    def add_stata_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        convert_dates: bool = ...,
        convert_categoricals: bool = ...,
        index_col: str | None = ...,
        convert_missing: bool = ...,
        preserve_dtypes: bool = ...,
        columns: Sequence[str] | None = ...,
        order_categoricals: bool = ...,
        chunksize: int | None = ...,
        iterator: bool = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> StataAsset: ...
    def add_xml_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = ...,
        order_by: SortersDefinition | None = ...,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        xpath: str = "./*",
        namespaces: typing.Dict[str, str] | None = ...,
        elems_only: bool = ...,
        attrs_only: bool = ...,
        names: Sequence[str] | None = ...,
        dtype: dict | None = ...,
        encoding: str | None = "utf-8",
        stylesheet: FilePath | None = ...,
        iterparse: typing.Dict[str, typing.List[str]] | None = ...,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = ...,
    ) -> XMLAsset: ...
