from typing import Union, List

import logging

from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

logger = logging.getLogger(__name__)


class PipelinePartitioner(Partitioner):
    DEFAULT_PARTITION_NAME: str = "IN_MEMORY_PARTITION"

    def __init__(
        self,
        name: str,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        runtime_keys: list = None
    ):
        logger.debug(f'Constructing PipelinePartitioner "{name}".')
        super().__init__(
            name=name,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            runtime_keys=runtime_keys
        )

    def _compute_partitions_for_data_asset(
        self,
        data_asset_name: str = None,
        *,
        runtime_parameters: Union[dict, None] = None,
        partition_config: dict = None,
    ) -> Union[List[Partition], None]:
        if partition_config["data_reference"] is None:
            return []
        if partition_config["name"] is None:
            if runtime_parameters:
                partition_config["name"] = self.DEFAULT_DELIMITER.join(
                    [str(value) for value in partition_config["definition"].values()]
                )
            else:
                partition_config["name"] = self.DEFAULT_PARTITION_NAME
        return [Partition(**partition_config)]
