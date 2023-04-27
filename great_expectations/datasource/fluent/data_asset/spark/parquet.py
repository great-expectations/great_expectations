from __future__ import annotations

import pydantic
from pydantic import Field
from typing_extensions import Literal

from great_expectations.datasource.fluent.file_path_data_asset import _FilePathDataAsset


class ParquetAsset(_FilePathDataAsset):
    type: Literal["parquet"] = "parquet"
    # header: bool = False
    # infer_schema: bool = Field(False, alias="InferSchema")
    merge_schema: bool = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        # TODO: Add others
        return {"mergeSchema"}

