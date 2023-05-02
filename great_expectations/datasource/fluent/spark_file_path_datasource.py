from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, List, Type, Union

import pydantic
from pydantic import Field
from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkDatasource
from great_expectations.datasource.fluent.file_path_data_asset import (
    _FilePathDataAsset,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import DataAsset


logger = logging.getLogger(__name__)


class CSVAsset(_FilePathDataAsset):
    # Overridden inherited instance fields
    type: Literal["csv"] = "csv"
    header: bool = False
    infer_schema: bool = Field(False, alias="InferSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        return {"header", "infer_schema"}


class DirectoryCSVAsset(_FilePathDataAsset):
    # Overridden inherited instance fields
    type: Literal["directory_csv"] = "directory_csv"
    data_directory: str
    header: bool = False
    infer_schema: bool = Field(False, alias="InferSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        # Reader method is still "csv"
        return self.type.replace("directory_", "")

    def _get_reader_options_include(self) -> set[str] | None:
        return {"data_directory", "header", "infer_schema"}




class ParquetAsset(_FilePathDataAsset):
    type: Literal["parquet"] = "parquet"
    # The options below are available for parquet as of spark v3.4.0
    # See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
    datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
        alias="datetimeRebaseMode"
    )
    int_96_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = Field(
        alias="int96RebaseMode"
    )
    merge_schema: bool = Field(False, alias="mergeSchema")

    class Config:
        extra = pydantic.Extra.forbid
        allow_population_by_field_name = True

    def _get_reader_method(self) -> str:
        return self.type

    def _get_reader_options_include(self) -> set[str] | None:
        """These options are available as of spark v3.4.0

        See https://spark.apache.org/docs/latest/sql-data-sources-parquet.html for more info.
        """
        return {"datetimeRebaseMode", "int96RebaseMode", "mergeSchema"}


_SPARK_FILE_PATH_ASSET_TYPES = Union[CSVAsset, DirectoryCSVAsset]
_SPARK_DIRECTORY_ASSET_CLASSES = (DirectoryCSVAsset,)


class _SparkFilePathDatasource(_SparkDatasource):
    # class attributes
    asset_types: ClassVar[List[Type[DataAsset]]] = [CSVAsset, DirectoryCSVAsset, ParquetAsset]

    # instance attributes
    assets: List[_SPARK_FILE_PATH_ASSET_TYPES] = []  # type: ignore[assignment]
