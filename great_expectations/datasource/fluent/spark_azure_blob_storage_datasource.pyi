from __future__ import annotations

import re
from logging import Logger
from typing import TYPE_CHECKING, Any, ClassVar, Type

from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,  # noqa: TCH001 # needed at runtime
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.spark_datasource import (
    SparkDatasourceError,
)

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient

    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        SortersDefinition,
    )
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        CSVAsset,
    )

logger: Logger

class SparkAzureBlobStorageDatasourceError(SparkDatasourceError): ...

class SparkAzureBlobStorageDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[S3DataConnector]] = S3DataConnector

    # instance attributes
    type: Literal["spark_abs"] = "spark_abs"

    # Azure Blob Storage specific attributes
    azure_options: dict[str, ConfigStr | Any] = {}
    # private
    _azure_client: BlobServiceClient | None
    def add_csv_asset(
        self,
        name: str,
        *,
        batch_metadata: BatchMetadata | None = ...,
        batching_regex: re.Pattern | str = r".*",
        abs_container: str = ...,
        abs_name_starts_with: str = "",
        abs_delimiter: str = "/",
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: SortersDefinition | None = ...,
    ) -> CSVAsset: ...
