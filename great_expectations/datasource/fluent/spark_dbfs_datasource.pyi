from __future__ import annotations

import re
from logging import Logger
from typing import TYPE_CHECKING, Optional

from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api as public_api
from great_expectations.datasource.fluent import SparkFilesystemDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector as DBFSDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition as SortersDefinition,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError as TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
    )

logger: Logger

class SparkDBFSDatasource(SparkFilesystemDatasource):
    type: Literal["spark_dbfs"]  # type: ignore[assignment]

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
