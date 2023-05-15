from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Type, Union

import pydantic
from typing_extensions import Literal

from great_expectations.compatibility import google
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import GCSUrl
from great_expectations.datasource.fluent import _PandasFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,  # noqa: TCH001 # needed at runtime
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    GoogleCloudStorageDataConnector,
)
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.pandas_datasource import (
    PandasDatasourceError,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.file_path_data_asset import (
        _FilePathDataAsset,
    )

logger = logging.getLogger(__name__)


class PandasGoogleCloudStorageDatasourceError(PandasDatasourceError):
    pass


@public_api
class PandasGoogleCloudStorageDatasource(_PandasFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[
        Type[GoogleCloudStorageDataConnector]
    ] = GoogleCloudStorageDataConnector

    # instance attributes
    type: Literal["pandas_gcs"] = "pandas_gcs"

    # Google Cloud Storage specific attributes
    bucket_or_name: str
    gcs_options: Dict[str, Union[ConfigStr, Any]] = {}

    _gcs_client: Union[google.Client, None] = pydantic.PrivateAttr(default=None)

    def _get_gcs_client(self) -> google.Client:
        gcs_client: Union[google.Client, None] = self._gcs_client
        if not gcs_client:
            # Validate that "google" libararies were successfully imported and attempt to create "gcs_client" handle.
            if google.service_account and google.storage:
                try:
                    credentials: Union[
                        google.Credentials, None
                    ] = None  # If configured with gcloud CLI / env vars
                    if "filename" in self.gcs_options:
                        filename: str = str(self.gcs_options.pop("filename"))
                        credentials = google.service_account.Credentials.from_service_account_file(
                            filename=filename
                        )
                    elif "info" in self.gcs_options:
                        info: Any = self.gcs_options.pop("info")
                        credentials = google.service_account.Credentials.from_service_account_info(
                            info=info
                        )

                    gcs_client = google.storage.Client(
                        credentials=credentials, **self.gcs_options
                    )
                except Exception as e:
                    # Failure to create "gcs_client" is most likely due invalid "gcs_options" dictionary.
                    raise PandasGoogleCloudStorageDatasourceError(
                        f'Due to exception: "{str(e)}", "gcs_client" could not be created.'
                    ) from e
            else:
                raise PandasGoogleCloudStorageDatasourceError(
                    'Unable to create "PandasGoogleCloudStorageDatasource" due to missing google dependency.'
                )

            self._gcs_client = gcs_client

        return gcs_client

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasGoogleCloudStorageDatasource.

        Args:
            test_assets: If assets have been passed to the PandasGoogleCloudStorageDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            _ = self._get_gcs_client()
        except Exception as e:
            raise TestConnectionError(
                "Attempt to connect to datasource failed with the following error message: "
                f"{str(e)}"
            ) from e

        if self.assets and test_assets:
            for asset in self.assets:
                asset.test_connection()

    def _build_data_connector(
        self,
        data_asset: _FilePathDataAsset,
        gcs_prefix: str = "",
        gcs_delimiter: str = "/",
        gcs_max_results: int = 1000,
        **kwargs,
    ) -> None:
        """Builds and attaches the `GoogleCloudStorageDataConnector` to the asset."""
        if kwargs:
            raise TypeError(
                f"_build_data_connector() got unexpected keyword arguments {list(kwargs.keys())}"
            )
        data_asset._data_connector = self.data_connector_type.build_data_connector(
            datasource_name=self.name,
            data_asset_name=data_asset.name,
            gcs_client=self._get_gcs_client(),
            batching_regex=data_asset.batching_regex,
            bucket_or_name=self.bucket_or_name,
            prefix=gcs_prefix,
            delimiter=gcs_delimiter,
            max_results=gcs_max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )

        # build a more specific `_test_connection_error_message`
        data_asset._test_connection_error_message = (
            self.data_connector_type.build_test_connection_error_message(
                data_asset_name=data_asset.name,
                batching_regex=data_asset.batching_regex,
                bucket_or_name=self.bucket_or_name,
                prefix=gcs_prefix,
                delimiter=gcs_delimiter,
            )
        )
