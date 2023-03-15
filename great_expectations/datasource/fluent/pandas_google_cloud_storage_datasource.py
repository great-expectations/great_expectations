from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pydantic
from typing_extensions import Literal

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
from great_expectations.datasource.fluent.pandas_file_path_datasource import (
    CSVAsset,
    ExcelAsset,
    JSONAsset,
    ParquetAsset,
)
from great_expectations.datasource.fluent.signatures import _merge_signatures

if TYPE_CHECKING:
    from google.cloud.storage.client import Client as GoogleCloudStorageClient
    from google.oauth2.service_account import (
        Credentials as GoogleServiceAccountCredentials,
    )

    from great_expectations.datasource.fluent.interfaces import (
        Sorter,
        SortersDefinition,
    )


logger = logging.getLogger(__name__)


GCS_IMPORTED = False
try:
    from google.cloud import storage  # noqa: disable=E0602
    from google.oauth2 import service_account  # noqa: disable=E0602

    GCS_IMPORTED = True
except ImportError:
    pass


class PandasGoogleCloudStorageDatasourceError(PandasDatasourceError):
    pass


class PandasGoogleCloudStorageDatasource(_PandasFilePathDatasource):
    # instance attributes
    type: Literal["pandas_gcs"] = "pandas_gcs"

    # Google Cloud Storage specific attributes
    bucket_or_name: str
    gcs_options: Dict[str, Union[ConfigStr, Any]] = {}

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
                        filename: str = str(self.gcs_options.pop("filename"))
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
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Union[re.Pattern, str],
        prefix: str = "",
        delimiter: str = "/",
        max_results: int = 1000,
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasGoogleCloudStorageDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches CSV filenames that is used to label the batches
            prefix (str): Google Cloud Storage object name prefix
            delimiter (str): Google Cloud Storage object name delimiter
            max_results (int): Google Cloud Storage max_results (default is 1000)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)

        asset = CSVAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        asset._data_connector = GoogleCloudStorageDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            gcs_client=self._get_gcs_client(),
            batching_regex=batching_regex_pattern,
            bucket_or_name=self.bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )
        asset._test_connection_error_message = (
            GoogleCloudStorageDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                bucket_or_name=self.bucket_or_name,
                prefix=prefix,
                delimiter=delimiter,
            )
        )
        return self.add_asset(asset=asset)

    def add_excel_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_results: int = 1000,
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasGoogleCloudStorageDatasource" object.

        Args:
            name: The name of the Excel asset
            batching_regex: regex pattern that matches Excel filenames that is used to label the batches
            prefix (str): Google Cloud Storage object name prefix
            delimiter (str): Google Cloud Storage object name delimiter
            max_results (int): Google Cloud Storage max_results (default is 1000)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = ExcelAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )
        asset._data_connector = GoogleCloudStorageDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            gcs_client=self._get_gcs_client(),
            batching_regex=batching_regex_pattern,
            bucket_or_name=self.bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )
        asset._test_connection_error_message = (
            GoogleCloudStorageDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                bucket_or_name=self.bucket_or_name,
                prefix=prefix,
                delimiter=delimiter,
            )
        )
        return self.add_asset(asset=asset)

    def add_json_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_results: int = 1000,
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasGoogleCloudStorageDatasource" object.

        Args:
            name: The name of the JSON asset
            batching_regex: regex pattern that matches JSON filenames that is used to label the batches
            prefix (str): Google Cloud Storage object name prefix
            delimiter (str): Google Cloud Storage object name delimiter
            max_results (int): Google Cloud Storage max_results (default is 1000)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = JSONAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )
        asset._data_connector = GoogleCloudStorageDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            gcs_client=self._get_gcs_client(),
            batching_regex=batching_regex_pattern,
            bucket_or_name=self.bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )
        asset._test_connection_error_message = (
            GoogleCloudStorageDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                bucket_or_name=self.bucket_or_name,
                prefix=prefix,
                delimiter=delimiter,
            )
        )
        return self.add_asset(asset=asset)

    def add_parquet_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_results: int = 1000,
        order_by: Optional[SortersDefinition] = None,
        **kwargs,
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasGoogleCloudStorageDatasource" object.

        Args:
            name: The name of the Parquet asset
            batching_regex: regex pattern that matches Parquet filenames that is used to label the batches
            prefix (str): Google Cloud Storage object name prefix
            delimiter (str): Google Cloud Storage object name delimiter
            max_results (int): Google Cloud Storage max_results (default is 1000)
            order_by: sorting directive via either list[Sorter] or "+/- key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = ParquetAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )
        asset._data_connector = GoogleCloudStorageDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            gcs_client=self._get_gcs_client(),
            batching_regex=batching_regex_pattern,
            bucket_or_name=self.bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
            file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
        )
        asset._test_connection_error_message = (
            GoogleCloudStorageDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                bucket_or_name=self.bucket_or_name,
                prefix=prefix,
                delimiter=delimiter,
            )
        )
        return self.add_asset(asset=asset)

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_json_asset.__signature__ = _merge_signatures(add_json_asset, JSONAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_parquet_asset.__signature__ = _merge_signatures(add_parquet_asset, ParquetAsset, exclude={"type"})  # type: ignore[attr-defined]
