from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Optional, Union

from typing_extensions import Literal

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import DBFSPath
from great_expectations.datasource.fluent import SparkFilesystemDatasource
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector,
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


@public_api
class SparkDBFSDatasource(SparkFilesystemDatasource):
    """Spark based Datasource for DataBricks File System (DBFS) based data assets."""

    # instance attributes
    # overridden from base `Literal['spark_filesystem']`
    type: Literal["spark_dbfs"] = "spark_dbfs"  # type: ignore[assignment] # base class has different type

    @public_api
    def add_csv_asset(
        self,
        name: str,
        batching_regex: Optional[Union[re.Pattern, str]] = None,
        glob_directive: str = "**/*",
        header: bool = False,
        infer_schema: bool = False,
        order_by: Optional[SortersDefinition] = None,
    ) -> CSVAsset:
        """Adds a CSV DataAsset to the present "SparkDBFSDatasource" object.

        Args:
            name: The name of the CSV asset
            batching_regex: regex pattern that matches csv filenames that is used to label the batches
            glob_directive: glob for selecting files in DBFS directory (defaults to `**/*`) or nested directories (e.g. `*/*/*.csv`)
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
        asset._data_connector = DBFSDataConnector.build_data_connector(
            datasource_name=self.name,
            data_asset_name=name,
            batching_regex=batching_regex_pattern,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_protocol_version,
        )
        asset._test_connection_error_message = (
            DBFSDataConnector.build_test_connection_error_message(
                data_asset_name=name,
                batching_regex=batching_regex_pattern,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
        return self._add_asset(asset=asset)
