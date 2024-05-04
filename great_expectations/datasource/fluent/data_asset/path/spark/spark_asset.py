from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, Type, Union

from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import (
    CSVAsset,
    DirectoryCSVAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.delta_asset import (
    DeltaAsset,
    DirectoryDeltaAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.json_asset import (
    DirectoryJSONAsset,
    JSONAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.orc_asset import (
    DirectoryORCAsset,
    ORCAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.parquet_asset import (
    DirectoryParquetAsset,
    ParquetAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.text_asset import (
    DirectoryTextAsset,
    TextAsset,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import DataAsset

# New asset types should be added to the SPARK_PATH_ASSET_TYPES tuple,
# and to SPARK_PATH_ASSET_UNION
# so that the schemas are generated and the assets are registered.


SPARK_PATH_ASSET_TYPES: Sequence[Type[DataAsset]] = (
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    DirectoryParquetAsset,
    ORCAsset,
    DirectoryORCAsset,
    JSONAsset,
    DirectoryJSONAsset,
    TextAsset,
    DirectoryTextAsset,
    DeltaAsset,
    DirectoryDeltaAsset,
)
SPARK_PATH_ASSET_UNION = Union[
    CSVAsset,
    DirectoryCSVAsset,
    ParquetAsset,
    DirectoryParquetAsset,
    ORCAsset,
    DirectoryORCAsset,
    JSONAsset,
    DirectoryJSONAsset,
    TextAsset,
    DirectoryTextAsset,
    DeltaAsset,
    DirectoryDeltaAsset,
]
