from __future__ import annotations

from typing import Optional, Union

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)


class _SparkGenericFilePathAssetMixin(PathDataAsset):
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
