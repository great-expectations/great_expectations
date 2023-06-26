import logging
from typing import Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector import (
    InferredAssetFilesystemDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class InferredAssetDBFSDataConnector(InferredAssetFilesystemDataConnector):
    """An Inferred Asset Data Connector used to connect to data on a DBFS filesystem.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        base_directory: The directory from which the Data Connector should read files.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        default_regex: A regex configuration for filtering data references.
        glob_directive: A glob pattern for selecting files in directory.
        sorters: A list of sorters for sorting data references.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        execution_engine: ExecutionEngine,
        default_regex: Optional[dict] = None,
        glob_directive: str = "*",
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing InferredAssetDBFSDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            base_directory=base_directory,
            execution_engine=execution_engine,
            default_regex=default_regex,
            glob_directive=glob_directive,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None
    ) -> str:
        full_path = super()._get_full_file_path(
            path=path, data_asset_name=data_asset_name
        )
        template_arguments: dict = {
            "path": full_path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
