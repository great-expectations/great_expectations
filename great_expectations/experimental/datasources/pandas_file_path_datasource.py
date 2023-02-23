from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    List,
    Type,
)

from great_expectations.experimental.datasources.dynamic_pandas import (
    _generate_pandas_data_asset_models,
)
from great_expectations.experimental.datasources.file_path_data_asset import (
    _FilePathDataAsset,
)
from great_expectations.experimental.datasources.pandas_datasource import (
    _PandasDatasource,
)

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import DataAsset


logger = logging.getLogger(__name__)


_PANDAS_FILE_TYPE_READER_METHOD_UNSUPPORTED_LIST = (
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

_FILE_PATH_ASSET_MODELS = _generate_pandas_data_asset_models(
    _FilePathDataAsset,
    blacklist=_PANDAS_FILE_TYPE_READER_METHOD_UNSUPPORTED_LIST,
    use_docstring_from_method=True,
    skip_first_param=True,
)

try:
    # variables only needed for type-hinting
    CSVAsset = _FILE_PATH_ASSET_MODELS["csv"]
    ExcelAsset = _FILE_PATH_ASSET_MODELS["excel"]
    JSONAsset = _FILE_PATH_ASSET_MODELS["json"]
    ORCAsset = _FILE_PATH_ASSET_MODELS["orc"]
    ParquetAsset = _FILE_PATH_ASSET_MODELS["parquet"]
except KeyError as key_err:
    logger.info(f"zep - {key_err} asset model could not be generated")
    CSVAsset = _FilePathDataAsset
    ExcelAsset = _FilePathDataAsset
    JSONAsset = _FilePathDataAsset
    ORCAsset = _FilePathDataAsset
    ParquetAsset = _FilePathDataAsset


class _PandasFilePathDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(
        _FILE_PATH_ASSET_MODELS.values()
    )

    # instance attributes
    assets: Dict[str, _FilePathDataAsset] = {}
