from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Type,
)

from great_expectations.datasource.fluent.data_asset.path.pandas.pandas import (
    _FILE_PATH_ASSET_MODELS,
)
from great_expectations.datasource.fluent.pandas_datasource import (
    _PandasDatasource,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.file_path_asset_base import _RegexDataAssetBase
    from great_expectations.datasource.fluent.interfaces import DataAsset


class _PandasFilePathDatasource(_PandasDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = list(_FILE_PATH_ASSET_MODELS.values())

    # instance attributes
    assets: List[_RegexDataAssetBase] = []
