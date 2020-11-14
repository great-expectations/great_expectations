from typing import List, Optional, Iterator
import copy

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector import DataConnector
from great_expectations.execution_environment.data_connector import (
    SinglePartitionerDataConnector,
)
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


# TODO: <Alex>Is this class still useful?  If not, we can deprecate it and replace it with SinglePartitionFilesystemDataConnector in all the test modues.</Alex>
# TODO: <Alex>Decision: Delete this class and rewrite the tests that rely on it in the way that exercises the relevant surviving classes.</Alex>
class SinglePartitionerDictDataConnector(SinglePartitionerDataConnector):
    def __init__(
        self,
        name: str,
        data_reference_dict: dict = None,
        sorters: List[dict] = None,
        **kwargs,
    ):
        if data_reference_dict is None:
            data_reference_dict = {}
        logger.debug(f'Constructing SinglePartitionerDictDataConnector "{name}".')
        super().__init__(
            name=name, sorters=sorters, **kwargs,
        )

        # This simulates the underlying filesystem
        self.data_reference_dict = data_reference_dict

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        data_reference_keys: List[str] = list(self.data_reference_dict.keys())
        data_reference_keys.sort()
        return data_reference_keys
