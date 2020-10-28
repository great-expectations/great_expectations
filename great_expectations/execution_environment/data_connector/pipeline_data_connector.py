from typing import Union, List, Any

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinitionSubset,
    BatchSpec
)
from great_expectations.execution_environment.types.batch_spec import InMemoryBatchSpec

logger = logging.getLogger(__name__)


class PipelineDataConnector(DataConnector):
    DEFAULT_DATA_ASSET_NAME: str = "IN_MEMORY_DATA_ASSET"

    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine: ExecutionEngine = None,
    ):
        logger.debug(f'Constructing PipelineDataConnector "{name}".')
        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
        )
