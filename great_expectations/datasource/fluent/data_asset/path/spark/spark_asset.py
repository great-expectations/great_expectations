from __future__ import annotations

from typing import Optional, Union

from pydantic import Field

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.file_path_data_asset import (
    _FilePathDataAsset,
)
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

# New asset types should be added to the _SPARK_FILE_PATH_ASSET_TYPES tuple,
# and to _SPARK_FILE_PATH_ASSET_TYPES_UNION
# so that the schemas are generated and the assets are registered.


class _SparkGenericFilePathAssetMixin(_FilePathDataAsset):
    # vvv Docs <> Source Code mismatch
    # ignoreCorruptFiles and ignoreMissingFiles appear in the docs https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html
    # but not in any reader method signatures (e.g. https://github.com/apache/spark/blob/v3.4.0/python/pyspark/sql/readwriter.py#L604)
    # ignore_corrupt_files: bool = Field(alias="ignoreCorruptFiles")
    # ignore_missing_files: bool = Field(alias="ignoreMissingFiles")
    # ^^^ Docs <> Source Code mismatch

    path_glob_filter: Optional[Union[bool, str]] = Field(None, alias="pathGlobFilter")
    recursive_file_lookup: Optional[Union[bool, str]] = Field(None, alias="recursiveFileLookup")
    modified_before: Optional[Union[bool, str]] = Field(None, alias="modifiedBefore")
    modified_after: Optional[Union[bool, str]] = Field(None, alias="modifiedAfter")

    @override
    def _get_reader_options_include(self) -> set[str]:
        return {
            "path_glob_filter",
            "recursive_file_lookup",
            "modified_before",
            "modified_after",
            # vvv Missing from method signatures but appear in documentation:
            # "ignoreCorruptFiles",
            # "ignore_missing_files",
            # ^^^ Missing from method signatures but appear in documentation:
        }


_SPARK_FILE_PATH_ASSET_TYPES = (
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
_SPARK_FILE_PATH_ASSET_TYPES_UNION = Union[
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
