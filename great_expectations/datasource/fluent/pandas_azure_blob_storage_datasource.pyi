import re
from _typeshed import Incomplete
from great_expectations.core.util import AzureUrl as AzureUrl
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import AzureBlobStorageDataConnector as AzureBlobStorageDataConnector
from great_expectations.datasource.fluent.interfaces import Sorter as Sorter, SortersDefinition as SortersDefinition, TestConnectionError as TestConnectionError
from great_expectations.datasource.fluent.pandas_datasource import PandasDatasourceError as PandasDatasourceError
from great_expectations.datasource.fluent.pandas_file_path_datasource import CSVAsset as CSVAsset, ExcelAsset as ExcelAsset, JSONAsset as JSONAsset, ParquetAsset as ParquetAsset
from typing import Any, Dict, Optional, Union
from typing_extensions import Literal

logger: Incomplete
ABS_IMPORTED: bool

class PandasAzureBlobStorageDatasourceError(PandasDatasourceError): ...

class PandasAzureBlobStorageDatasource(_PandasFilePathDatasource):
    type: Literal['pandas_abs']
    azure_options: Dict[str, Any]
    def test_connection(self, test_assets: bool = ...) -> None: ...
    def add_csv_asset(self, name: str, batching_regex: Union[re.Pattern, str], container: str, name_starts_with: str = ..., delimiter: str = ..., order_by: Optional[SortersDefinition] = ..., **kwargs) -> CSVAsset: ...
    def add_excel_asset(self, name: str, batching_regex: Union[re.Pattern, str], container: str, name_starts_with: str = ..., delimiter: str = ..., order_by: Optional[SortersDefinition] = ..., **kwargs) -> ExcelAsset: ...
    def add_json_asset(self, name: str, batching_regex: Union[re.Pattern, str], container: str, name_starts_with: str = ..., delimiter: str = ..., order_by: Optional[SortersDefinition] = ..., **kwargs) -> JSONAsset: ...
    def add_parquet_asset(self, name: str, batching_regex: Union[re.Pattern, str], container: str, name_starts_with: str = ..., delimiter: str = ..., order_by: Optional[SortersDefinition] = ..., **kwargs) -> ParquetAsset: ...
