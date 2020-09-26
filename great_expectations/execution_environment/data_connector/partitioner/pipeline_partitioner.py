import logging
from typing import List, Any
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition

logger = logging.getLogger(__name__)


class PipelinePartitioner(Partitioner):
    def __init__(
        self,
        name: str,
        data_connector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        config_params: dict = None,
        **kwargs
    ):
        logger.debug(f'Constructing PipelinePartitioner "{name}".')
        super().__init__(
            name=name,
            data_connector=data_connector,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            config_params=config_params,
            **kwargs
        )

    def _compute_partitions_for_data_asset(
        self,
        data_asset_name: str = None,
        *,
        in_memory_dataset: Any = None,
        pipeline_data_asset_name: str = None,
        pipeline_partition_name: str = None,
    ) -> List[Partition]:
        return [
            Partition(
                name=pipeline_partition_name,
                definition={pipeline_partition_name: in_memory_dataset},
                source=in_memory_dataset,
                data_asset_name=data_asset_name or pipeline_data_asset_name
            )
        ]
