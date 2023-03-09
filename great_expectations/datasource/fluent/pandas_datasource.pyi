import os
import sqlite3
from typing import (
    AbstractSet,
    Any,
    Callable,
    ClassVar,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
import pydantic
import sqlalchemy
from _typeshed import Incomplete
from typing_extensions import Literal

from great_expectations.core.batch_spec import (
    PandasBatchSpec as PandasBatchSpec,
)
from great_expectations.core.batch_spec import (
    RuntimeDataBatchSpec as RuntimeDataBatchSpec,
)
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME as _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.constants import (
    _FIELDS_ALWAYS_SET as _FIELDS_ALWAYS_SET,
)
from great_expectations.datasource.fluent.dynamic_pandas import (
    _generate_pandas_data_asset_models as _generate_pandas_data_asset_models,
)
from great_expectations.datasource.fluent.interfaces import (
    Batch as Batch,
)
from great_expectations.datasource.fluent.interfaces import (
    BatchRequest as BatchRequest,
)
from great_expectations.datasource.fluent.interfaces import (
    BatchRequestOptions as BatchRequestOptions,
)
from great_expectations.datasource.fluent.interfaces import (
    DataAsset as DataAsset,
)
from great_expectations.datasource.fluent.interfaces import (
    Datasource as Datasource,
)
from great_expectations.datasource.fluent.interfaces import (
    _DataAssetT as _DataAssetT,
)
from great_expectations.datasource.fluent.signatures import (
    _merge_signatures as _merge_signatures,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME as DEFAULT_PANDAS_DATA_ASSET_NAME,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine as PandasExecutionEngine,
)
from great_expectations.util import NotImported as NotImported
from great_expectations.validator.validator import Validator as Validator

MappingIntStrAny = Mapping[Union[int, str], Any]
AbstractSetIntStr = AbstractSet[Union[int, str]]
logger: Incomplete
_PandasDataFrameT = TypeVar("_PandasDataFrameT")

class PandasDatasourceError(Exception): ...

class _PandasDataAsset(DataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[Set[str]]

    class Config:
        extra: Incomplete
    def _get_reader_method(self) -> str: ...
    def test_connection(self) -> None: ...
    def batch_request_options_template(self) -> BatchRequestOptions: ...
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]: ...
    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = ...
    ) -> BatchRequest: ...
    def _validate_batch_request(self, batch_request: BatchRequest) -> None: ...
    def json(
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
        **dumps_kwargs: Any
    ) -> str: ...

class ClipboardAsset(_PandasDataAsset): ...
class CSVAsset(_PandasDataAsset): ...
class ExcelAsset(_PandasDataAsset): ...
class FeatherAsset(_PandasDataAsset): ...
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
    dataframe: _PandasDataFrameT

    class Config:
        extra: Incomplete
    def _validate_dataframe(cls, dataframe: pd.DataFrame) -> pd.DataFrame: ...
    def _get_reader_method(self) -> str: ...
    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> list[Batch]: ...

class _PandasDatasource(Datasource):
    asset_types: ClassVar[Sequence[Type[DataAsset]]]
    assets: MutableMapping[str, _DataAssetT]
    @property
    def execution_engine_type(self) -> Type[PandasExecutionEngine]: ...
    def test_connection(self, test_assets: bool = ...) -> None: ...
    def json(
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
        **dumps_kwargs: Any
    ) -> str: ...

class PandasDatasource(_PandasDatasource):
    asset_types: ClassVar[Sequence[Type[DataAsset]]]
    _data_context: Incomplete
    type: Literal["pandas"]
    assets: Dict[str, _PandasDataAsset]
    def test_connection(self, test_assets: bool = ...) -> None: ...
    def _get_validator(self, asset: _PandasDataAsset) -> Validator: ...
    def add_dataframe_asset(
        self, name: str, dataframe: pd.DataFrame
    ) -> DataFrameAsset: ...
    def read_dataframe(
        self, dataframe: pd.DataFrame, asset_name: Optional[str] = ...
    ) -> Validator: ...
    def add_clipboard_asset(self, name: str, **kwargs) -> ClipboardAsset: ...
    def read_clipboard(
        self, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_csv_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> CSVAsset: ...
    def read_csv(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_excel_asset(
        self, name: str, io: Union[str, bytes, os.PathLike], **kwargs
    ) -> ExcelAsset: ...
    def read_excel(
        self,
        io: Union[str, bytes, os.PathLike],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_feather_asset(
        self, name: str, path: pydantic.FilePath, **kwargs
    ) -> FeatherAsset: ...
    def read_feather(
        self, path: pydantic.FilePath, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_gbq_asset(self, name: str, query: str, **kwargs) -> GBQAsset: ...
    def read_gbq(
        self, query: str, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_hdf_asset(
        self, name: str, path_or_buf: Union[str, os.PathLike, pd.HDFStore], **kwargs
    ) -> HDFAsset: ...
    def read_hdf(
        self,
        path_or_buf: Union[str, os.PathLike, pd.HDFStore],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_html_asset(
        self, name: str, io: Union[str, os.PathLike], **kwargs
    ) -> HTMLAsset: ...
    def read_html(
        self, io: Union[str, os.PathLike], asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_json_asset(
        self, name: str, path_or_buf: Union[pydantic.Json, pydantic.FilePath], **kwargs
    ) -> JSONAsset: ...
    def read_json(
        self,
        path_or_buf: Union[pydantic.Json, pydantic.FilePath],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_orc_asset(
        self, name: str, path: pydantic.FilePath, **kwargs
    ) -> ORCAsset: ...
    def read_orc(
        self, path: pydantic.FilePath, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_parquet_asset(
        self, name: str, path: pydantic.FilePath, **kwargs
    ) -> ParquetAsset: ...
    def read_parquet(
        self, path: pydantic.FilePath, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_pickle_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> PickleAsset: ...
    def read_pickle(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_sas_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> SASAsset: ...
    def read_sas(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_spss_asset(
        self, name: str, path: pydantic.FilePath, **kwargs
    ) -> SPSSAsset: ...
    def read_spss(
        self, path: pydantic.FilePath, asset_name: Optional[str] = ..., **kwargs
    ) -> Validator: ...
    def add_sql_asset(
        self,
        name: str,
        sql: Union[str, sqlalchemy.select, sqlalchemy.text],
        con: Union[str, sqlalchemy.engine.Engine, sqlite3.Connection],
        **kwargs
    ) -> SQLAsset: ...
    def read_sql(
        self,
        sql: Union[str, sqlalchemy.select, sqlalchemy.text],
        con: Union[str, sqlalchemy.engine.Engine, sqlite3.Connection],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_sql_query_asset(
        self,
        name: str,
        sql: Union[str, sqlalchemy.select, sqlalchemy.text],
        con: Union[str, sqlalchemy.engine.Engine, sqlite3.Connection],
        **kwargs
    ) -> SQLQueryAsset: ...
    def read_sql_query(
        self,
        sql: Union[str, sqlalchemy.select, sqlalchemy.text],
        con: Union[str, sqlalchemy.engine.Engine, sqlite3.Connection],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_sql_table_asset(
        self,
        name: str,
        table_name: str,
        con: Union[str, sqlalchemy.engine.Engine],
        **kwargs
    ) -> SQLTableAsset: ...
    def read_sql_table(
        self,
        table_name: str,
        con: Union[str, sqlalchemy.engine.Engine],
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_stata_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> StataAsset: ...
    def read_stata(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_table_asset(
        self, name: str, filepath_or_buffer: pydantic.FilePath, **kwargs
    ) -> TableAsset: ...
    def read_table(
        self,
        filepath_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
    def add_xml_asset(
        self, name: str, path_or_buffer: pydantic.FilePath, **kwargs
    ) -> XMLAsset: ...
    def read_xml(
        self,
        path_or_buffer: pydantic.FilePath,
        asset_name: Optional[str] = ...,
        **kwargs
    ) -> Validator: ...
