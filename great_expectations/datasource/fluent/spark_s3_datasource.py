from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Literal, Type, Union

import pydantic

from great_expectations.compatibility import aws
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import S3Url
from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_datasource import (
    SparkDatasourceError,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        _SPARK_FILE_PATH_ASSET_TYPES_UNION,
    )


logger = logging.getLogger(__name__)


class SparkS3DatasourceError(SparkDatasourceError):
    pass


@public_api
class SparkS3Datasource(_SparkFilePathDatasource):
    # class attributes
    data_connector_type: ClassVar[Type[S3DataConnector]] = S3DataConnector
    # these fields should not be passed to the execution engine
    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[set] = {
        "bucket",
        "boto3_options",
    }

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
            if aws.boto3:
                _check_config_substitutions_needed(
                    self, self.boto3_options, raise_warning_if_provider_not_present=True
                )
                # pull in needed config substitutions using the `_config_provider`
                # The `FluentBaseModel.dict()` call will do the config substitution on the serialized dict if a `config_provider` is passed.
                boto3_options: dict = self.dict(
                    config_provider=self._config_provider
                ).get("boto3_options", {})
                try:
                    s3_client = aws.boto3.client("s3", **boto3_options)
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
            for asset in self.assets:
                asset.test_connection()

    def _build_data_connector(  # noqa: PLR0913
        self,
        data_asset: _SPARK_FILE_PATH_ASSET_TYPES_UNION,
        s3_prefix: str = "",
        s3_delimiter: str = "/",
        s3_max_keys: int = 1000,
        s3_recursive_file_discovery: bool = False,
        **kwargs,
    ) -> None:
        """Builds and attaches the `S3DataConnector` to the asset."""
        if kwargs:
            raise TypeError(
                f"_build_data_connector() got unexpected keyword arguments {list(kwargs.keys())}"
            )

        data_asset._data_connector = self.data_connector_type.build_data_connector(
            datasource_name=self.name,
            data_asset_name=data_asset.name,
            s3_client=self._get_s3_client(),
            batching_regex=data_asset.batching_regex,
            bucket=self.bucket,
            prefix=s3_prefix,
            delimiter=s3_delimiter,
            max_keys=s3_max_keys,
            recursive_file_discovery=s3_recursive_file_discovery,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )

        # build a more specific `_test_connection_error_message`
        data_asset._test_connection_error_message = (
            self.data_connector_type.build_test_connection_error_message(
                data_asset_name=data_asset.name,
                batching_regex=data_asset.batching_regex,
                bucket=self.bucket,
                prefix=s3_prefix,
                delimiter=s3_delimiter,
                recursive_file_discovery=s3_recursive_file_discovery,
            )
        )
