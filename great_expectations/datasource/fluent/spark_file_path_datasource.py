from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    List,
    Sequence,
    Type,
)

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.data_asset.path.spark.spark_asset import (
    _SPARK_FILE_PATH_ASSET_TYPES,
    _SPARK_FILE_PATH_ASSET_TYPES_UNION,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import DataAsset


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = _SPARK_FILE_PATH_ASSET_TYPES

    # instance attributes
    assets: List[_SPARK_FILE_PATH_ASSET_TYPES_UNION] = []
