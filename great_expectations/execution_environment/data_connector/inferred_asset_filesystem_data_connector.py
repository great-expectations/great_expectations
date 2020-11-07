from typing import List, Optional, Iterator
import copy

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector import InferredAssetFilePathDataConnector
from great_expectations.execution_environment.data_connector.sorter import Sorter
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
)
from great_expectations.execution_environment.data_connector.partition_query import (
    PartitionQuery,
    build_partition_query,
)
from great_expectations.execution_environment.types import PathBatchSpec
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    map_data_reference_string_to_batch_definition_list_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
    get_filesystem_one_level_directory_glob_path_list,
    build_sorters_from_config,
)
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class InferredAssetFilesystemDataConnector(InferredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        base_directory: str = None,
        default_regex: dict = None,
        glob_directive: str = "*",
        execution_engine: ExecutionEngine = None,
        sorters: List[dict] = None,
    ):
        logger.debug(f'Constructing InferredAssetFilesystemDataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            base_directory=base_directory,
            glob_directive=glob_directive,
            default_regex=default_regex,
            sorters=sorters,
        )

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=self.base_directory,
            glob_directive=self.glob_directive
        )
        return path_list

    def build_batch_spec(
        self,
        batch_definition: BatchDefinition
    ) -> PathBatchSpec:
        batch_spec = super().build_batch_spec(batch_definition=batch_definition)
        return PathBatchSpec(batch_spec)
