from __future__ import annotations

import logging
import pathlib
import re
from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import Literal

from great_expectations.datasource.fluent import _SparkFilePathDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import (
        Sorter,
        SortersDefinition,
    )

logger = logging.getLogger(__name__)


class SparkFilesystemDatasource(_SparkFilePathDatasource):
    # instance attributes
    type: Literal["spark_filesystem"] = "spark_filesystem"

    base_directory: pathlib.Path
    data_context_root_directory: Optional[pathlib.Path] = None

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SparkDatasource.

        Args:
            test_assets: If assets have been passed to the SparkDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        if not self.base_directory.exists():
            raise TestConnectionError(
                f"base_directory path: {self.base_directory.resolve()} does not exist."
            )

        if self.assets and test_assets:
            for asset in self.assets.values():
                asset.test_connection()

    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = None,
        glob_directive: str = "**/*",
        header: bool = False,
        infer_schema: bool = False,
        order_by: Optional[SortersDefinition] = None,
    ) -> CSVAsset:
        """Adds a CSV DataAsst to the present "SparkFilesystemDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
            header: boolean (default False) indicating whether or not first line of CSV file is header line
            infer_schema: boolean (default False) instructing Spark to attempt to infer schema of CSV file heuristically
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
        asset._data_connector = FilesystemDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
        )
        asset._test_connection_error_message = (
            FilesystemDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self.add_asset(asset=asset)
