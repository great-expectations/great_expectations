from logging import Logger
from typing import ClassVar, List, Type

from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.interfaces import DataAsset as DataAsset
from great_expectations.datasource.fluent.pandas_datasource import _PandasDatasource

logger: Logger

class _PandasFilePathDatasource(_PandasDatasource):
    asset_types: ClassVar[List[Type[DataAsset]]]
    assets: List[FileDataAsset]
