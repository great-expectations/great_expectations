from __future__ import annotations

from typing import Type

from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.dynamic_pandas import _generate_pandas_data_asset_models

_PANDAS_FILE_TYPE_READER_METHOD_UNSUPPORTED_LIST = (
    # "read_csv",
    # "read_json",
    # "read_excel",
    # "read_parquet",
    "read_clipboard",  # not path based
    # "read_feather",
    # "read_fwf",
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
_FILE_PATH_ASSET_MODELS = _generate_pandas_data_asset_models(
    FileDataAsset,
    blacklist=_PANDAS_FILE_TYPE_READER_METHOD_UNSUPPORTED_LIST,
    use_docstring_from_method=True,
    skip_first_param=True,
)
CSVAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("csv", FileDataAsset)
ExcelAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("excel", FileDataAsset)
FWFAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("fwf", FileDataAsset)
JSONAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("json", FileDataAsset)
ORCAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("orc", FileDataAsset)
ParquetAsset: Type[FileDataAsset] = _FILE_PATH_ASSET_MODELS.get("parquet", FileDataAsset)
