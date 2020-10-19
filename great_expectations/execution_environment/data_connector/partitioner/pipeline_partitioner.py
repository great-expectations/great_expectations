import logging
from typing import Any, Dict, List, Union

from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.execution_environment.data_connector.partitioner.partitioner import (
    Partitioner,
)

logger = logging.getLogger(__name__)


class PipelinePartitioner(Partitioner):
    def __init__(
        self,
        name: str,
        data_connector,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        config_params: dict = None,
        **kwargs,
    ):
        logger.debug(f'Constructing PipelinePartitioner "{name}".')
        super().__init__(
            name=name,
            data_connector=data_connector,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            config_params=config_params,
            **kwargs,
        )

    def _compute_partitions_for_data_asset(
        self,
        data_asset_name: str = None,
        *,
        pipeline_data_asset_name: str = None,
        pipeline_datasets: List[Dict[str, Union[str, Any]]] = None,
    ) -> Union[List[Partition], None]:
        if pipeline_datasets is None:
            return None
        return [
            Partition(
                name=pipeline_dataset["partition_name"],
                data_asset_name=data_asset_name or pipeline_data_asset_name,
                definition={
                    pipeline_dataset["partition_name"]: pipeline_dataset[
                        "data_reference"
                    ]
                },
                data_reference=pipeline_dataset["data_reference"],
            )
            for pipeline_dataset in pipeline_datasets
        ]
