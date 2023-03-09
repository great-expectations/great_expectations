from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pydantic
from typing_extensions import Literal

from great_expectations.core.util import S3Url
from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,  # noqa: TCH001 # needed at runtime
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.spark_datasource import (
    SparkDatasourceError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.datasource.fluent.interfaces import (
        Sorter,
        SortersDefinition,
    )


logger = logging.getLogger(__name__)


BOTO3_IMPORTED = False
try:
    import boto3  # noqa: disable=E0602

    BOTO3_IMPORTED = True
except ImportError:
    pass


class SparkS3DatasourceError(SparkDatasourceError):
    pass


class SparkS3Datasource(_SparkFilePathDatasource):
    # instance attributes
    type: Literal["spark_s3"] = "spark_s3"

    # S3 specific attributes
    bucket: str
    boto3_options: Dict[str, Union[ConfigStr, Any]] = {}

    _s3_client: Union[BaseClient, None] = pydantic.PrivateAttr(default=None)

    def _get_s3_client(self) -> BaseClient:
        s3_client: Union[BaseClient, None] = self._s3_client
        if not s3_client:
            # Validate that "boto3" libarary was successfully imported and attempt to create "s3_client" handle.
            if BOTO3_IMPORTED:
                try:
                    s3_client = boto3.client("s3", **self.boto3_options)
                except Exception as e:
                    # Failure to create "s3_client" is most likely due invalid "boto3_options" dictionary.
                    raise SparkS3DatasourceError(
                        f'Due to exception: "{str(e)}", "s3_client" could not be created.'
                    ) from e
            else:
                raise SparkS3DatasourceError(
                    'Unable to create "SparkS3Datasource" due to missing boto3 dependency.'
                )

            self._s3_client = s3_client

        return s3_client

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkS3Datasource.

        Args:
            test_assets: If assets have been passed to the SparkS3Datasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            _ = self._get_s3_client()
        except Exception as e:
            raise TestConnectionError(
                "Attempt to connect to datasource failed with the following error message: "
                f"{str(e)}"
            ) from e

        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[str, re.Pattern]] = None,
        header: bool = False,
        infer_schema: bool = False,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[SortersDefinition] = None,
    ) -> CSVAsset:
        """Adds a CSV DataAsst to the present "SparkS3Datasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches CSV filenames that is used to label the batches
            header: boolean (default False) indicating whether or not first line of CSV file is header line
            infer_schema: boolean (default False) instructing Spark to attempt to infer schema of CSV file heuristically
            prefix: S3 prefix
            delimiter: S3 delimiter
            max_keys: S3 max_keys (default is 1000)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = CSVAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            header=header,
            inferSchema=infer_schema,
            order_by=order_by_sorters,
        )
        asset._data_connector = S3DataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            s3_client=self._get_s3_client(),
            batching_regex=batching_regex_pattern,
            bucket=self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )
        asset._test_connection_error_message = (
            S3DataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                bucket=self.bucket,
                prefix=prefix,
                delimiter=delimiter,
            )
        )
        return self.add_asset(asset=asset)
