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


class ParquetAssetBase(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
    merge_schema: Optional[Union[bool, str]] = Field(None, alias="mergeSchema")
    datetime_rebase_mode: Optional[Literal["EXCEPTION", "CORRECTED", "LEGACY"]] = Field(
        None, alias="datetimeRebaseMode"
    )
    int_96_rebase_mode: Optional[Literal["EXCEPTION", "CORRECTED", "LEGACY"]] = Field(
        None, alias="int96RebaseMode"
    )

    class Config:
        extra = "forbid"

        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "parquet"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()
            ._get_reader_options_include()
            .union(
                {
                    "datetime_rebase_mode",
                    "int_96_rebase_mode",
                    "merge_schema",
                }
            )
        )


class ParquetAsset(FileDataAsset, ParquetAssetBase):
    type: Literal["parquet"] = "parquet"


class DirectoryParquetAsset(DirectoryDataAsset, ParquetAssetBase):
    type: Literal["directory_parquet"] = "directory_parquet"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "parquet"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
