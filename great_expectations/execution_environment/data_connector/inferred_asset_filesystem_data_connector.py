import copy
import logging
from typing import Iterator, List, Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector import (
    SinglePartitionerDataConnector,
)
from great_expectations.execution_environment.data_connector.data_connector import (
    DataConnector,
)
from great_expectations.execution_environment.data_connector.partition_query import (
    PartitionQuery,
    build_partition_query,
)
from great_expectations.execution_environment.data_connector.sorter import Sorter
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    build_sorters_from_config,
    get_filesystem_one_level_directory_glob_path_list,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)
from great_expectations.execution_environment.types import PathBatchSpec

logger = logging.getLogger(__name__)


class InferredAssetFilesystemDataConnector(SinglePartitionerDataConnector):
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

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        path_list: List[str] = get_filesystem_one_level_directory_glob_path_list(
            base_directory_path=self.base_directory, glob_directive=self.glob_directive
        )
        return path_list

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        path: str = self._map_batch_definition_to_data_reference(
            batch_definition=batch_definition
        )
        if not path:
            raise ValueError(
                f"""No data reference for data asset name "{batch_definition.data_asset_name}" matches the given
partition definition {batch_definition.partition_definition} from batch definition {batch_definition}.
                """
            )
        return {"path": path}

    def _build_batch_spec_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> PathBatchSpec:
        batch_spec = super()._build_batch_spec_from_batch_definition(
            batch_definition=batch_definition
        )
        return PathBatchSpec(batch_spec)
