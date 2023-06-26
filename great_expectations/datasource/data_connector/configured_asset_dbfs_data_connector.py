import logging
from typing import Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class ConfiguredAssetDBFSDataConnector(ConfiguredAssetFilesystemDataConnector):
    """
    Extension of ConfiguredAssetFilesystemDataConnector used to connect to the DataBricks File System (DBFS).

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        name (str): required name for DataConnector
        datasource_name (str): required name for datasource
        assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector). These can each have their own regex and sorters
        execution_engine (ExecutionEngine): Reference to ExecutionEngine
        default_regex (dict): optional regex configuration for filtering data_references
        glob_directive (str): glob for selecting files in directory (defaults to *)
        sorters (list): optional list of sorters for sorting data_references
        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        base_directory: str,
        assets: dict,
        execution_engine: ExecutionEngine,
        default_regex: Optional[dict] = None,
        glob_directive: str = "**/*",
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing ConfiguredAssetDBFSDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            base_directory=base_directory,
            assets=assets,
            execution_engine=execution_engine,
            default_regex=default_regex,
            glob_directive=glob_directive,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        full_path = super()._get_full_file_path_for_asset(path=path, asset=asset)
        template_arguments: dict = {
            "path": full_path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
