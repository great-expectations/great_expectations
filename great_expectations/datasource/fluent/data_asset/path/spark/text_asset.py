from __future__ import annotations

from typing import Literal, Optional

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.directory_asset import (
    DirectoryDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.data_asset.path.spark.spark_generic import (
    _SparkGenericFilePathAssetMixin,
)


class TextAssetBase(_SparkGenericFilePathAssetMixin):
    # The options below are available as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
    wholetext: bool = Field(False)
    line_sep: Optional[str] = Field(None, alias="lineSep")

    class Config:
        extra = "forbid"

        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "text"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return super()._get_reader_options_include().union({"wholetext", "line_sep"})


class TextAsset(FileDataAsset, TextAssetBase):
    type: Literal["text"] = "text"


class DirectoryTextAsset(DirectoryDataAsset, TextAssetBase):
    type: Literal["directory_text"] = "directory_text"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "text"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-text.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
