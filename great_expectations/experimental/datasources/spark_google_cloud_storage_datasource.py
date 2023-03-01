from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pydantic
from typing_extensions import Literal

from great_expectations.core.util import GCSUrl
from great_expectations.experimental.datasources import _SparkFilePathDatasource
from great_expectations.experimental.datasources.data_asset.data_connector import (
    DataConnector,
    GoogleCloudStorageDataConnector,
)
from great_expectations.experimental.datasources.interfaces import TestConnectionError
from great_expectations.experimental.datasources.spark_datasource import (
    SparkDatasourceError,
)
from great_expectations.experimental.datasources.spark_file_path_datasource import (
    CSVAsset,
)

if TYPE_CHECKING:
    from google.cloud.storage.client import Client as GoogleCloudStorageClient
    from google.oauth2.service_account import (
        Credentials as GoogleServiceAccountCredentials,
    )

    from great_expectations.experimental.datasources.interfaces import (
        BatchSorter,
        BatchSortersDefinition,
    )


logger = logging.getLogger(__name__)


GCS_IMPORTED = False
try:
    from google.cloud import storage  # noqa: disable=E0602
    from google.oauth2 import service_account  # noqa: disable=E0602

    GCS_IMPORTED = True
except ImportError:
    pass


class SparkGoogleCloudStorageDatasourceError(SparkDatasourceError):
    pass


class SparkGoogleCloudStorageDatasource(_SparkFilePathDatasource):
    # instance attributes
    type: Literal["spark_gcs"] = "spark_gcs"

    # Google Cloud Storage specific attributes
    bucket_or_name: str
    gcs_options: Dict[str, Any] = {}

    _gcs_client: Union[GoogleCloudStorageClient, None] = pydantic.PrivateAttr(
        default=None
    )

    def _get_gcs_client(self) -> GoogleCloudStorageClient:
        gcs_client: Union[GoogleCloudStorageClient, None] = self._gcs_client
        if not gcs_client:
            # Validate that "google" libararies were successfully imported and attempt to create "gcs_client" handle.
            if GCS_IMPORTED:
                try:
                    credentials: Union[
                        GoogleServiceAccountCredentials, None
                    ] = None  # If configured with gcloud CLI / env vars
                    if "filename" in self.gcs_options:
                        filename: str = self.gcs_options.pop("filename")
                        credentials = (
                            service_account.Credentials.from_service_account_file(
                                filename=filename
                            )
                        )
                    elif "info" in self.gcs_options:
                        info: Any = self.gcs_options.pop("info")
                        credentials = (
                            service_account.Credentials.from_service_account_info(
                                info=info
                            )
                        )

                    gcs_client = storage.Client(
                        credentials=credentials, **self.gcs_options
                    )
                except Exception as e:
                    # Failure to create "gcs_client" is most likely due invalid "gcs_options" dictionary.
                    raise SparkGoogleCloudStorageDatasourceError(
                        f'Due to exception: "{str(e)}", "gcs_client" could not be created.'
                    ) from e
            else:
                raise SparkGoogleCloudStorageDatasourceError(
                    'Unable to create "SparkGoogleCloudStorageDatasource" due to missing google dependency.'
                )

            self._gcs_client = gcs_client

        return gcs_client

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkGoogleCloudStorageDatasource.

        Args:
            test_assets: If assets have been passed to the SparkGoogleCloudStorageDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if self._gcs_client is None:
            raise TestConnectionError(
                "Unable to load google.cloud.storage.client (it is required for SparkGoogleCloudStorageDatasource)."
            )

        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Union[re.Pattern, str],
        prefix: str = "",
        delimiter: str = "/",
        max_results: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> CSVAsset:
        """Adds a CSV DataAsst to the present "SparkGoogleCloudStorageDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            prefix (str): Google Cloud Storage object name prefix
            delimiter (str): Google Cloud Storage object name delimiter
            max_results (int): Google Cloud Storage max_results (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = CSVAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
        )

        data_connector: DataConnector = GoogleCloudStorageDataConnector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            gcs_client=self._gcs_client,
            bucket_or_name=self.bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )
        test_connection_error_message: str = f"""No file in bucket "{self.bucket_or_name}" with prefix "{prefix}" matched regular expressions pattern "{batching_regex_pattern.pattern}" using delimiter "{delimiter}" for DataAsset "{name}"."""
        return self.add_asset(
            asset=asset,
            data_connector=data_connector,
            test_connection_error_message=test_connection_error_message,
        )
