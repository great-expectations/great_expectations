from __future__ import annotations

import pathlib
import re
from logging import Logger
from typing import TYPE_CHECKING, ClassVar, Optional, Type

from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import BatchMetadata
    from great_expectations.datasource.fluent.interfaces import (
        SortersDefinition,
    )
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
        DirectoryCSVAsset,
    )

logger: Logger

class SparkFilesystemDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[FilesystemDataConnector]] = ...

    # instance attributes
    type: Literal["spark_filesystem"] = "spark_filesystem"

    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None
    def add_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        glob_directive: str = "**/*",
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> CSVAsset: ...
    def add_directory_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        glob_directive: str = "**/*",
        data_directory: str,
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> DirectoryCSVAsset: ...
