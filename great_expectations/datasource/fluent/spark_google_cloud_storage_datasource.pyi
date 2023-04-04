from __future__ import annotations

import re
from logging import Logger
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Type

from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,  # noqa: TCH001 # needed at runtime
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    GoogleCloudStorageDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition,
)

if TYPE_CHECKING:
    from google.cloud.storage.client import Client as GoogleCloudStorageClient

    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
    )

logger: Logger

class SparkGoogleCloudStorageDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[
        Type[GoogleCloudStorageDataConnector]
    ] = GoogleCloudStorageDataConnector

    # instance attributes
    type: Literal["spark_gcs"] = "spark_gcs"

    # GCS specific attributes
    bucket_or_name: str
    gcs_options: dict[str, ConfigStr | Any] = {}

    _gcs_client: GoogleCloudStorageClient | None
    def add_csv_asset(
        self,
        name: str,
        *,
        batching_regex: re.Pattern | str = r".*",
        gcs_prefix: str = "",
        gcs_delimiter: str = "/",
        gcs_max_results: int = 1000,
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> CSVAsset: ...
