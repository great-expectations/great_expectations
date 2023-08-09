import re
from logging import Logger
from typing import Any, ClassVar, Literal, Optional, Type

from great_expectations.compatibility import google
from great_expectations.datasource.fluent import BatchMetadata, _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    GoogleCloudStorageDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

logger: Logger

class SparkGoogleCloudStorageDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[GoogleCloudStorageDataConnector]] = ...

    # instance attributes
    type: Literal["spark_gcs"] = "spark_gcs"

    # GCS specific attributes
    bucket_or_name: str
    gcs_options: dict[str, ConfigStr | Any] = {}

    _gcs_client: google.Client | None
    def add_csv_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        gcs_prefix: str = "",
        gcs_delimiter: str = "/",
        gcs_max_results: int = 1000,
        gcs_recursive_file_discovery: bool = False,
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> CSVAsset: ...
