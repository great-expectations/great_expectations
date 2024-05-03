from __future__ import annotations

from typing import Literal, Optional

from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.data_asset.path.directory_asset import (
    DirectoryDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.file_asset import FileDataAsset
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)


class DeltaAssetBase(PathDataAsset):
    # The options below are available as of 2023-05-12
    # See https://docs.databricks.com/delta/tutorial.html for more info.

    timestamp_as_of: Optional[str] = Field(None, alias="timestampAsOf")
    version_as_of: Optional[str] = Field(None, alias="versionAsOf")

    class Config:
        extra = "forbid"

        allow_population_by_field_name = True

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "delta"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """The options below are available as of 2023-05-12

        See https://docs.databricks.com/delta/tutorial.html for more info.
        """
        return {"timestamp_as_of", "version_as_of"}


class DeltaAsset(FileDataAsset, DeltaAssetBase):
    type: Literal["delta"] = "delta"


class DirectoryDeltaAsset(DirectoryDataAsset, DeltaAssetBase):
    type: Literal["directory_delta"] = "directory_delta"

    @classmethod
    @override
    def _get_reader_method(cls) -> str:
        return "delta"

    @override
    def _get_reader_options_include(self) -> set[str]:
        """The options below are available as of 2023-05-12

        See https://docs.databricks.com/delta/tutorial.html for more info.
        """
        return (
            super()._get_reader_options_include()
            | super(DirectoryDataAsset, self)._get_reader_options_include()
        )
