import logging
from typing import Optional

from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class ConfiguredAssetDBFSDataConnector(ConfiguredAssetFilesystemDataConnector):
    """
    Extension of ConfiguredAssetFilesystemDataConnector used to connect to the DataBricks File System (DBFS). Note: This works for the current implementation of DBFS. If in the future DBFS diverges from a Filesystem-like implementation, we should instead inherit from ConfiguredAssetFilePathDataConnector or another DataConnector.

    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, splitting and sampling, or other techniques appropriate
    for obtaining batches of data.

    The ConfiguredAssetDBFSDataConnector is one of two classes (InferredAssetDBFSDataConnector being the
    other one) designed for connecting to data on DBFS.

    A ConfiguredAssetDBFSDataConnector requires an explicit specification of each DataAsset you want to connect to.
    This allows more fine-tuning, but also requires more setup.
    """

    def __init__(
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
        """
        ConfiguredAssetDataConnector for connecting to DBFS. This class supports the configuration of default_regex
        and sorters for filtering and sorting data_references. It takes in configured `assets` as a dictionary.

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

        return self.execution_engine.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            template_arguments=template_arguments,
        )
