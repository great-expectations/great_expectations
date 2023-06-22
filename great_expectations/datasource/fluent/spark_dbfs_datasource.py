from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Literal, Type

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.util import DBFSPath
from great_expectations.datasource.fluent import (
    SparkFilesystemDatasource,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    DBFSDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.spark_file_path_datasource import (
        _SPARK_FILE_PATH_ASSET_TYPES_UNION,
    )

logger = logging.getLogger(__name__)


@public_api
class SparkDBFSDatasource(SparkFilesystemDatasource):
    """Spark based Datasource for DataBricks File System (DBFS) based data assets."""

    # class attributes
    data_connector_type: ClassVar[Type[DBFSDataConnector]] = DBFSDataConnector

    # instance attributes
    # overridden from base `Literal['spark_filesystem']`
    type: Literal["spark_dbfs"] = "spark_dbfs"  # type: ignore[assignment] # base class has different type

    def _build_data_connector(
        self,
        data_asset: _SPARK_FILE_PATH_ASSET_TYPES_UNION,
        glob_directive: str = "**/*",
        **kwargs,
    ) -> None:
        """Builds and attaches the `DBFSDataConnector` to the asset."""
        if kwargs:
            raise TypeError(
                f"_build_data_connector() got unexpected keyword arguments {list(kwargs.keys())}"
            )
        data_asset._data_connector = self.data_connector_type.build_data_connector(
            datasource_name=self.name,
            data_asset_name=data_asset.name,
            batching_regex=data_asset.batching_regex,
            base_directory=self.base_directory,
            glob_directive=glob_directive,
            data_context_root_directory=self.data_context_root_directory,
            file_path_template_map_fn=DBFSPath.convert_to_protocol_version,
        )

        # build a more specific `_test_connection_error_message`
        data_asset._test_connection_error_message = (
            self.data_connector_type.build_test_connection_error_message(
                data_asset_name=data_asset.name,
                batching_regex=data_asset.batching_regex,
                glob_directive=glob_directive,
                base_directory=self.base_directory,
            )
        )
