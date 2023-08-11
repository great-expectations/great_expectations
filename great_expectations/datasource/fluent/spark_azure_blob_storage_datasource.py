from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Final, Literal, Type, Union

import pydantic

from great_expectations.compatibility import azure
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import AzureUrl
from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    AzureBlobStorageDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_datasource import (
    SparkDatasourceError,
)

logger = logging.getLogger(__name__)


_MISSING: Final = object()

if TYPE_CHECKING:
    from great_expectations.compatibility.azure import BlobServiceClient
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        _SPARK_FILE_PATH_ASSET_TYPES_UNION,
    )


class SparkAzureBlobStorageDatasourceError(SparkDatasourceError):
    pass


@public_api
class SparkAzureBlobStorageDatasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[
        Type[AzureBlobStorageDataConnector]
    ] = AzureBlobStorageDataConnector

    # instance attributes
    type: Literal["spark_abs"] = "spark_abs"

    # Azure Blob Storage specific attributes
    azure_options: Dict[str, Union[ConfigStr, Any]] = {}

    _account_name: str = pydantic.PrivateAttr(default="")
    # on 3.11 the annotation must be type-checking import otherwise it will fail at import time
    _azure_client: Union[BlobServiceClient, None] = pydantic.PrivateAttr(default=None)

    def _get_azure_client(self) -> azure.BlobServiceClient:
        azure_client: Union[azure.BlobServiceClient, None] = self._azure_client
        if not azure_client:
            _check_config_substitutions_needed(
                self, self.azure_options, raise_warning_if_provider_not_present=True
            )
            # pull in needed config substitutions using the `_config_provider`
            # The `FluentBaseModel.dict()` call will do the config substitution on the serialized dict if a `config_provider` is passed.
            azure_options: dict = self.dict(config_provider=self._config_provider).get(
                "azure_options", {}
            )

            # Thanks to schema validation, we are guaranteed to have one of `conn_str` or `account_url` to
            # use in authentication (but not both). If the format or content of the provided keys is invalid,
            # the assignment of `self._account_name` and `self._azure_client` will fail and an error will be raised.
            conn_str: str | None = azure_options.get("conn_str")
            account_url: str | None = azure_options.get("account_url")
            if not bool(conn_str) ^ bool(account_url):
                raise SparkAzureBlobStorageDatasourceError(
                    "You must provide one of `conn_str` or `account_url` to the `azure_options` key in your config (but not both)"
                )

            # Validate that "azure" libararies were successfully imported and attempt to create "azure_client" handle.
            if azure.BlobServiceClient:  # type: ignore[truthy-function] # False if NotImported
                try:
                    if conn_str is not None:
                        self._account_name = re.search(  # type: ignore[union-attr] # re.search could return None
                            r".*?AccountName=(.+?);.*?", conn_str
                        ).group(
                            1
                        )
                        azure_client = azure.BlobServiceClient.from_connection_string(
                            **azure_options
                        )
                    elif account_url is not None:
                        self._account_name = re.search(  # type: ignore[union-attr] # re.search could return None
                            r"(?:https?://)?(.+?).blob.core.windows.net",
                            account_url,
                        ).group(
                            1
                        )
                        azure_client = azure.BlobServiceClient(**azure_options)
                except Exception as e:
                    # Failure to create "azure_client" is most likely due invalid "azure_options" dictionary.
                    raise SparkAzureBlobStorageDatasourceError(
                        f'Due to exception: "{str(e)}", "azure_client" could not be created.'
                    ) from e
            else:
                raise SparkAzureBlobStorageDatasourceError(
                    'Unable to create "SparkAzureBlobStorageDatasource" due to missing azure.storage.blob dependency.'
                )

            self._azure_client = azure_client

        if not azure_client:
            raise SparkAzureBlobStorageDatasourceError(
                "Failed to return `azure_client`"
            )

        return azure_client

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkAzureBlobStorageDatasource.

        Args:
            test_assets: If assets have been passed to the SparkAzureBlobStorageDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            _ = self._get_azure_client()
        except Exception as e:
            raise TestConnectionError(
                "Attempt to connect to datasource failed with the following error message: "
                f"{str(e)}"
            ) from e

        if self.assets and test_assets:
            for asset in self.assets:
                asset.test_connection()

    def _build_data_connector(  # noqa: PLR0913
        self,
        data_asset: _SPARK_FILE_PATH_ASSET_TYPES_UNION,
        abs_container: str = _MISSING,  # type: ignore[assignment] # _MISSING is used as sentinel value
        abs_name_starts_with: str = "",
        abs_delimiter: str = "/",
        abs_recursive_file_discovery: bool = False,
        **kwargs,
    ) -> None:
        """Builds and attaches the `AzureBlobStorageDataConnector` to the asset."""
        if kwargs:
            raise TypeError(
                f"_build_data_connector() got unexpected keyword arguments {list(kwargs.keys())}"
            )
        if abs_container is _MISSING:
            raise TypeError(
                f"'{data_asset.name}' is missing required argument 'abs_container'"
            )

        data_asset._data_connector = self.data_connector_type.build_data_connector(
            datasource_name=self.name,
            data_asset_name=data_asset.name,
            azure_client=self._get_azure_client(),
            batching_regex=data_asset.batching_regex,
            account_name=self._account_name,
            container=abs_container,
            name_starts_with=abs_name_starts_with,
            delimiter=abs_delimiter,
            recursive_file_discovery=abs_recursive_file_discovery,
            file_path_template_map_fn=AzureUrl.AZURE_BLOB_STORAGE_WASBS_URL_TEMPLATE.format,
        )

        # build a more specific `_test_connection_error_message`
        data_asset._test_connection_error_message = (
            self.data_connector_type.build_test_connection_error_message(
                data_asset_name=data_asset.name,
                batching_regex=data_asset.batching_regex,
                account_name=self._account_name,
                container=abs_container,
                name_starts_with=abs_name_starts_with,
                delimiter=abs_delimiter,
                recursive_file_discovery=abs_recursive_file_discovery,
            )
        )
