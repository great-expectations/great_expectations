from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Type,
)

from great_expectations.datasource.fluent.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    _PandasDatasource,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import DataAsset


logger = logging.getLogger(__name__)


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
    _FilePathDataAsset,
    blacklist=_PANDAS_FILE_TYPE_READER_METHOD_UNSUPPORTED_LIST,
    use_docstring_from_method=True,
    skip_first_param=True,
)

CSVAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "csv", _FilePathDataAsset
)
ExcelAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "excel", _FilePathDataAsset
)
FWFAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "fwf", _FilePathDataAsset
)
JSONAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "json", _FilePathDataAsset
)
ORCAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "orc", _FilePathDataAsset
)
ParquetAsset: Type[_FilePathDataAsset] = _FILE_PATH_ASSET_MODELS.get(
    "parquet", _FilePathDataAsset
)


class _PandasFilePathDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(
        _FILE_PATH_ASSET_MODELS.values()
    )

    # instance attributes
    assets: List[_FilePathDataAsset] = []
