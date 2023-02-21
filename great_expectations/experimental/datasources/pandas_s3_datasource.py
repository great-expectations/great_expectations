from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Union

import pydantic
from typing_extensions import Literal

from great_expectations.experimental.datasources import _PandasFilePathDatasource
from great_expectations.experimental.datasources.data_asset.data_connector import (
    DataConnector,
    S3DataConnector,
)
from great_expectations.experimental.datasources.interfaces import (
    BatchSortersDefinition,
    TestConnectionError,
    _batch_sorter_from_list,
)
from great_expectations.experimental.datasources.pandas_file_path_datasource import (
    CSVAsset,
    ExcelAsset,
    JSONAsset,
    ParquetAsset,
)
from great_expectations.experimental.datasources.signatures import _merge_signatures

logger = logging.getLogger(__name__)


try:
    import boto3
    import botocore
    from botocore.client import BaseClient
except ImportError:
    logger.debug(
        "Unable to load boto3 or botocore; install optional boto3 and botocore dependencies for support."
    )
    boto3 = None
    botocore = None
    BaseClient = None


class PandasS3Datasource(_PandasFilePathDatasource):
    # instance attributes
    type: Literal["pandas_s3"] = "pandas_s3"
    name: str

    # S3 specific attributes
    bucket: str
    boto3_options: Dict[str, Any] = {}

    _s3_client: Optional[BaseClient] = pydantic.PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)

        try:
            self._s3_client = boto3.client("s3", **self.boto3_options)
        except (TypeError, AttributeError):
            self._s3_client = None

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the PandasS3Datasource.

        Args:
            test_assets: If assets have been passed to the PandasS3Datasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if self._s3_client is None:
            raise TestConnectionError(
                "Unable to load boto3 (it is required for PandasS3Datasource)."
            )

        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        regex: Union[re.Pattern, str],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> CSVAsset:  # type: ignore[valid-type]
        """Adds a CSV DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            prefix (str): S3 prefix
            delimiter (str): S3 delimiter
            max_keys (int): S3 max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_csv`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)

        asset = CSVAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )

        data_connector: DataConnector = S3DataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            regex=regex,
            s3_client=self._s3_client,
            bucket=self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        test_connection_error_message: str = f"""No file in bucket "{self.bucket}" with prefix "{prefix}" matched regular expressions pattern "{regex.pattern}" using deliiter "{delimiter}" for DataAsset "{name}"."""
        return self.add_asset(
            asset=asset,
            data_connector=data_connector,
            test_connection_error_message=test_connection_error_message,
        )

    def add_excel_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ExcelAsset:  # type: ignore[valid-type]
        """Adds an Excel DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            prefix (str): S3 prefix
            delimiter (str): S3 delimiter
            max_keys (int): S3 max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_excel`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)

        asset = ExcelAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )

        data_connector: DataConnector = S3DataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            regex=regex,
            s3_client=self._s3_client,
            bucket=self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        test_connection_error_message: str = f"""No file in bucket "{self.bucket}" with prefix "{prefix}" matched regular expressions pattern "{regex.pattern}" using deliiter "{delimiter}" for DataAsset "{name}"."""
        return self.add_asset(
            asset=asset,
            data_connector=data_connector,
            test_connection_error_message=test_connection_error_message,
        )

    def add_json_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> JSONAsset:  # type: ignore[valid-type]
        """Adds a JSON DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            prefix (str): S3 prefix
            delimiter (str): S3 delimiter
            max_keys (int): S3 max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_json`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)

        asset = JSONAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )

        data_connector: DataConnector = S3DataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            regex=regex,
            s3_client=self._s3_client,
            bucket=self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        test_connection_error_message: str = f"""No file in bucket "{self.bucket}" with prefix "{prefix}" matched regular expressions pattern "{regex.pattern}" using deliiter "{delimiter}" for DataAsset "{name}"."""
        return self.add_asset(
            asset=asset,
            data_connector=data_connector,
            test_connection_error_message=test_connection_error_message,
        )

    def add_parquet_asset(
        self,
        name: str,
        regex: Union[str, re.Pattern],
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        order_by: Optional[BatchSortersDefinition] = None,
        **kwargs,  # TODO: update signature to have specific keys & types
    ) -> ParquetAsset:  # type: ignore[valid-type]
        """Adds a Parquet DataAsst to the present "PandasS3Datasource" object.

        Args:
            name: The name of the csv asset
            regex: regex pattern that matches csv filenames that is used to label the batches
            prefix (str): S3 prefix
            delimiter (str): S3 delimiter
            max_keys (int): S3 max_keys (default is 1000)
            order_by: sorting directive via either list[BatchSorter] or "{+|-}key" syntax: +/- (a/de)scending; + default
            kwargs: Extra keyword arguments should correspond to ``pandas.read_parquet`` keyword args
        """
        if isinstance(regex, str):
            regex = re.compile(regex)

        asset = ParquetAsset(
            name=name,
            regex=regex,
            order_by=_batch_sorter_from_list(order_by or []),
            **kwargs,
        )

        data_connector: DataConnector = S3DataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            regex=regex,
            s3_client=self._s3_client,
            bucket=self.bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
        )
        test_connection_error_message: str = f"""No file in bucket "{self.bucket}" with prefix "{prefix}" matched regular expressions pattern "{regex.pattern}" using deliiter "{delimiter}" for DataAsset "{name}"."""
        return self.add_asset(
            asset=asset,
            data_connector=data_connector,
            test_connection_error_message=test_connection_error_message,
        )

    # attr-defined issue
    # https://github.com/python/mypy/issues/12472
    add_csv_asset.__signature__ = _merge_signatures(add_csv_asset, CSVAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_excel_asset.__signature__ = _merge_signatures(add_excel_asset, ExcelAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_json_asset.__signature__ = _merge_signatures(add_json_asset, JSONAsset, exclude={"type"})  # type: ignore[attr-defined]
    add_parquet_asset.__signature__ = _merge_signatures(add_parquet_asset, ParquetAsset, exclude={"type"})  # type: ignore[attr-defined]
