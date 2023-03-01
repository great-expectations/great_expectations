from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pydantic
from typing_extensions import Literal

from great_expectations.core.util import S3Url
from great_expectations.experimental.datasources import _PandasFilePathDatasource
from great_expectations.experimental.datasources.data_asset.data_connector import (
    S3DataConnector,
)
from great_expectations.experimental.datasources.interfaces import TestConnectionError
from great_expectations.experimental.datasources.pandas_datasource import (
    PandasDatasourceError,
)
from great_expectations.experimental.datasources.pandas_file_path_datasource import (
    CSVAsset,
    ExcelAsset,
    JSONAsset,
    ParquetAsset,
)
from great_expectations.experimental.datasources.signatures import _merge_signatures

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.experimental.datasources.interfaces import (
        BatchSorter,
        BatchSortersDefinition,
    )


logger = logging.getLogger(__name__)


BOTO3_IMPORTED = False
try:
    import boto3  # noqa: disable=E0602

    BOTO3_IMPORTED = True
except ImportError:
    pass


class PandasS3DatasourceError(PandasDatasourceError):
    pass


class PandasS3Datasource(_PandasFilePathDatasource):
    # instance attributes
    type: Literal["pandas_s3"] = "pandas_s3"

    # S3 specific attributes
    bucket: str
    boto3_options: Dict[str, Any] = {}

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
                    raise PandasS3DatasourceError(
                        f'Due to exception: "{str(e)}", "s3_client" could not be created.'
                    ) from e
            else:
                raise PandasS3DatasourceError(
                    'Unable to create "PandasS3Datasource" due to missing boto3 dependency.'
                )

            self._s3_client = s3_client

        return s3_client

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasS3Datasource.

        Args:
            test_assets: If assets have been passed to the PandasS3Datasource, whether to test them as well.

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

    def _build_data_connector(self, data_asset_name: str, **kwargs) -> None:
        """Builds "S3DataConnector", which links this Datasource and its DataAsset members to AWS S3.

        Args:
            data_asset_name: The name of the DataAsset using this DataConnector instance
            kwargs: Extra keyword arguments allow specification of arguments used by "S3DataConnector"
        """
        self._data_connector = S3DataConnector(
            datasource_name=self.name,
            data_asset_name=data_asset_name,
            s3_client=self._get_s3_client(),
            bucket=self.bucket,
            **kwargs,
        )

    def _build_test_connection_error_message(
        self, data_asset_name: str, **kwargs
    ) -> None:
        """Builds helpful error message for Datasource and its DataAsset members when connecting to AWS S3.

        Args:
            data_asset_name: The name of the DataAsset using this error message
            kwargs: Extra keyword arguments allow specification of arguments used by this error message's template
        """
        test_connection_error_message_template: str = 'No file in bucket "{bucket}" with prefix "{prefix}" matched regular expressions pattern "{batching_regex}" using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'
        self._test_connection_error_message = (
            test_connection_error_message_template.format(
                **(
                    {
                        "bucket": self.bucket,
                        "data_asset_name": data_asset_name,
                    }
                    | kwargs
                )
            )
        )

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Union[re.Pattern, str],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches CSV filenames that is used to label the batches
            prefix: S3 object name prefix
            delimiter: S3 object name delimiter
            max_keys: S3 max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
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
            **kwargs,
        )

        self._build_data_connector(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )
        self._build_test_connection_error_message(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        return self.add_asset(asset=asset)

    def add_excel_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the Excel asset
            batching_regex: regex pattern that matches Excel filenames that is used to label the batches
            prefix: S3 object name prefix
            delimiter: S3 object name delimiter
            max_keys: S3 object name max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = ExcelAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        self._build_data_connector(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )
        self._build_test_connection_error_message(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        return self.add_asset(asset=asset)

    def add_json_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the JSON asset
            batching_regex: regex pattern that matches JSON filenames that is used to label the batches
            prefix: S3 object name prefix
            delimiter: S3 object name delimiter
            max_keys: S3 object name max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = JSONAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        self._build_data_connector(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )
        self._build_test_connection_error_message(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        return self.add_asset(asset=asset)

    def add_parquet_asset(
        self,
        name: str,
        batching_regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the Parquet asset
            batching_regex: regex pattern that matches Parquet filenames that is used to label the batches
            prefix: S3 object name prefix
            delimiter: S3 object name delimiter
            max_keys: S3 object name max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        batching_regex_pattern: re.Pattern = self.parse_batching_regex_string(
            batching_regex=batching_regex
        )
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )

        asset = ParquetAsset(
            name=name,
            batching_regex=batching_regex_pattern,
            order_by=order_by_sorters,
            **kwargs,
        )

        self._build_data_connector(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
        )
        self._build_test_connection_error_message(
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        return self.add_asset(asset=asset)

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_json_asset.__signature__ = _merge_signatures(add_json_asset, JSONAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_parquet_asset.__signature__ = _merge_signatures(add_parquet_asset, ParquetAsset, exclude={"type"})  # type: ignore[attr-defined]
