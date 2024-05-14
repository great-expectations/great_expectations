from __future__ import annotations

from typing import Literal, Optional, Union

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.directory_asset import (
    DirectoryDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.data_asset.path.spark.spark_generic import (
    _SparkGenericFilePathAssetMixin,
)


class ORCAssetBase(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
    merge_schema: Optional[Union[bool, str]] = Field(False, alias="mergeSchema")

    class Config:
        extra = "forbid"

        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "orc"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return super()._get_reader_options_include().union({"merge_schema"})


class ORCAsset(FileDataAsset, ORCAssetBase):
    type: Literal["orc"] = "orc"


class DirectoryORCAsset(DirectoryDataAsset, ORCAssetBase):
    type: Literal["directory_orc"] = "directory_orc"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "orc"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-orc.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
