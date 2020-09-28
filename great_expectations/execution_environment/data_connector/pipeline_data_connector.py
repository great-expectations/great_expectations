import logging
from typing import List, Any

from great_expectations.execution_environment.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.core.id_dict import BatchSpec

logger = logging.getLogger(__name__)


class PipelineDataConnector(DataConnector):
    DEFAULT_DATA_ASSET_NAME: str = "IN_MEMORY_DATA_ASSET"
    DEFAULT_PARTITION_NAME: str = "IN_MEMORY_PARTITION"

    def __init__(
        self,
        name: str,
        execution_environment: ExecutionEnvironment,
        partitioners: dict = None,
        default_partitioner: str = None,
        assets: dict = None,
        config_params: dict = None,
        batch_definition_defaults: dict = None,
        in_memory_dataset: Any = None,
        **kwargs
    ):
        logger.debug("Constructing PipelineDataConnector {!r}".format(name))
        super().__init__(
            name=name,
            execution_environment=execution_environment,
            partitioners=partitioners,
            default_partitioner=default_partitioner,
            assets=assets,
            config_params=config_params,
            batch_definition_defaults=batch_definition_defaults,
            **kwargs
        )

        self._in_memory_dataset = in_memory_dataset

    @property
    def in_memory_dataset(self) -> Any:
        return self._in_memory_dataset

    @in_memory_dataset.setter
    def in_memory_dataset(self, in_memory_dataset: Any):
        self._in_memory_dataset = in_memory_dataset

    def get_available_data_asset_names(self) -> list:
        if self.assets:
            return list(self.assets.keys())
        return []

    def _get_available_partitions(
        self,
        partitioner: Partitioner,
        partition_name: str = None,
        data_asset_name: str = None,
        repartition: bool = False
    ) -> List[Partition]:
        data_asset_directives: dict = self._get_data_asset_directives(
            data_asset_name=data_asset_name,
            partition_name=partition_name
        )
        pipeline_data_asset_name: str = data_asset_directives["data_asset_name"]
        pipeline_partition_name: str = data_asset_directives["partition_name"]
        return partitioner.get_available_partitions(
            partition_name=partition_name,
            data_asset_name=data_asset_name,
            repartition=repartition,
            in_memory_dataset=self.in_memory_dataset,
            pipeline_data_asset_name=pipeline_data_asset_name,
            pipeline_partition_name=pipeline_partition_name,
        )

    def _get_data_asset_directives(self, data_asset_name: str, partition_name: str) -> dict:
        partition_name = partition_name or self.DEFAULT_PARTITION_NAME
        if (
            data_asset_name
            and self.assets
            and self.assets.get(data_asset_name)
            and self.assets[data_asset_name].get("config_params")
            and self.assets[data_asset_name]["config_params"]
        ):
            partition_name = self.assets[data_asset_name]["config_params"].get("partition_name", partition_name)
        elif not data_asset_name:
            data_asset_name = self.DEFAULT_DATA_ASSET_NAME
        return {"data_asset_name": data_asset_name, "partition_name": partition_name}

    def build_batch_spec_from_partitions(
        self,
        partitions: List[Partition],
        batch_definition: dict,
        batch_spec: dict = None
    ) -> BatchSpec:
        """
        Args:
            partitions:
            batch_definition:
            batch_spec:
        Returns:
            batch_spec
        """
        # TODO: <Alex>If the list has multiple elements, we are using the first one (TBD/TODO multifile config / multibatch)</Alex>
        in_memory_dataset: Any = partitions[0].source
        return self._build_batch_spec_from_in_memory_dataset(
            in_memory_dataset=in_memory_dataset,
            batch_definition=batch_definition,
            batch_spec=batch_spec
        )

    def _build_batch_spec_from_in_memory_dataset(
        self,
        in_memory_dataset: Any,
        batch_definition: dict,
        batch_spec: dict
    ) -> BatchSpec:
        batch_spec["in_memory_dataset"] = in_memory_dataset
        batch_spec = self._execution_environment.execution_engine.process_batch_definition(
            batch_definition=batch_definition, batch_spec=batch_spec
        )
        return BatchSpec(batch_spec)
