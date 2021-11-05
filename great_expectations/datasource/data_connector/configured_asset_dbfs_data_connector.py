import logging
import os
import re
from pathlib import Path
from typing import List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import PathBatchSpec
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
    normalize_directory_path,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

logger = logging.getLogger(__name__)


# TODO: AJB Alex: make this the same as AzureDataConnector and GCSDataConnector, S3DataConnector
#  i.e. copy those classes and override the same methods they do with my conversion from /dbfs to dbfs:
# TODO: AJB Inherit from ConfiguredAssetFilePathDataConnector or inferred version
# class ConfiguredAssetDBFSDataConnector(ConfiguredAssetFilesystemDataConnector):
#
#     @staticmethod
#     def _convert_path_to_dbfs_root(path: str) -> str:
#
#         # TODO: AJB Strengthen with regex to make sure there are no preceding tabs etc
#         if path.startswith("/dbfs"):
#             path = path.replace("/dbfs/foldername/", "dbfs:/foldername/file.csv", 1)
#             # os.listdir("/dbfs")
#         return path
#
#     def _generate_batch_spec_parameters_from_batch_definition(
#             self, batch_definition: BatchDefinition
#     ) -> dict:
#
#         unmodified_path = super()._generate_batch_spec_parameters_from_batch_definition(
#             batch_definition=batch_definition
#         )
#
#         modified_path = self._convert_path_to_dbfs_root(unmodified_path["path"])
#
#         return {"path": modified_path}


class ConfiguredAssetDBFSDataConnector(ConfiguredAssetFilePathDataConnector):
    """
    Extension of ConfiguredAssetFilePathDataConnector used to connect to the DataBricks File System (DBFS)

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
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        glob_directive: str = "**/*",
        sorters: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        """
        ConfiguredAssetDataConnector for connecting to DBFS. This class supports the configuration of default_regex
        and sorters for filtering and sorting data_references. It takes in configured `assets` as a dictionary.

        Args:
            name (str): required name for DataConnector
            datasource_name (str): required name for datasource
            assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector). These can each have their own regex and sorters
            execution_engine (ExecutionEngine): optional reference to ExecutionEngine
            default_regex (dict): optional regex configuration for filtering data_references
            glob_directive (str): glob for selecting files in directory (defaults to *)
            sorters (list): optional list of sorters for sorting data_references
            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
        """
        logger.debug(f'Constructing ConfiguredAssetDBFSDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        self._base_directory = base_directory
        self._glob_directive = glob_directive

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        base_directory: str = self.base_directory
        glob_directive: str = self._glob_directive

        if asset is not None:
            if asset.base_directory:
                base_directory = normalize_directory_path(
                    dir_path=asset.base_directory, root_directory_path=base_directory
                )
            if asset.glob_directive:
                glob_directive = asset.glob_directive

        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=base_directory, glob_directive=glob_directive
        )

        return sorted(path_list)

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        base_directory: str = self.base_directory
        if asset is not None:
            if asset.base_directory:
                base_directory = normalize_directory_path(
                    dir_path=asset.base_directory,
                    root_directory_path=base_directory,
                )
        full_path = str(Path(base_directory).joinpath(path))

        template_arguments: dict = {
            "path": full_path,
        }

        try:
            return self.execution_engine.resolve_data_reference(
                data_connector_name=self.__class__.__name__,
                template_arguments=template_arguments,
            )
        except AttributeError:
            raise ge_exceptions.DataConnectorError(
                "A non-existent/unknown ExecutionEngine instance was referenced."
            )

    @property
    def base_directory(self) -> str:
        """
        Accessor method for base_directory. If directory is a relative path, interpret it as relative to the
        root directory. If it is absolute, then keep as-is.
        """
        return normalize_directory_path(
            dir_path=self._base_directory,
            root_directory_path=self.data_context_root_directory,
        )
