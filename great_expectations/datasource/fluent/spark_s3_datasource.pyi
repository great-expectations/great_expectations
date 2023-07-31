import re
from logging import Logger
from typing import Any, ClassVar, Literal, Optional, Type

from great_expectations.datasource.fluent import BatchMetadata, _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

logger: Logger

class SparkS3Datasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[S3DataConnector]] = ...

    # instance attributes
    type: Literal["spark_s3"] = "spark_s3"

    # S3 specific attributes
    bucket: str
    boto3_options: dict[str, ConfigStr | Any] = {}
    def add_csv_asset(  # noqa: PLR0913
        self,
        name: str,
        *,
        batch_metadata: Optional[BatchMetadata] = ...,
        batching_regex: re.Pattern | str = r".*",
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        s3_recursive_file_discovery: bool = False,
        header: bool = ...,
        infer_schema: bool = ...,
        order_by: Optional[SortersDefinition] = ...,
    ) -> CSVAsset: ...
