# -*- coding: utf-8 -*-

import copy
import itertools
from typing import List, Dict, Union, Callable
from ruamel.yaml.comments import CommentedMap

import logging

from great_expectations.data_context.types.base import (
    PartitionerConfig,
    partitionerConfigSchema
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.no_op_partitioner import NoOpPartitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.partition_query import (
    PartitionQuery,
    build_partition_query
)
from great_expectations.core.id_dict import BatchSpec
from great_expectations.core.util import nested_update
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class DataConnector(object):
    DEFAULT_DATA_ASSET_NAME: str = "DEFAULT_DATA_ASSET"
    DEFAULT_PARTITION_NAME: str = "DEFAULT_PARTITION"

    r"""
    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the ExecutionEnvironment.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyExecutionEnvironment
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and
    ready for validation.

    A batch is a sample from a data asset, sliced according to a particular rule. For
    example, an hourly slide of the Events table or “most recent `users` records.”

    A Batch is the primary unit of validation in the Great Expectations DataContext.
    Batches include metadata that identifies how they were constructed--the same “batch_spec”
    assembled by the data connector, While not every ExecutionEnvironment will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.
    """
    _default_reader_options: dict = {}

    #NOTE Abe 20201011 : This looks like a type defintion for BatchSpec, not a property of DataConnector
    recognized_batch_definition_keys: set = {
        "execution_environment",
        "data_connector",
        "data_asset_name",
        "partition_query",
        "batch_spec_passthrough",
        "limit",
    }

    def __init__(
        self,
        name: str,
        partitioners: dict = None,
        default_partitioner: str = None,
        assets: dict = None,
        config_params: dict = None,
        batch_definition_defaults: dict = None,
        execution_engine: ExecutionEngine = None,
        data_context_root_directory: str = None,
        **kwargs
    ):
        self._name = name

        # TODO: <Alex>Is this needed?  Where do these batch_definition_come_from and what are the values?</Alex>
        batch_definition_defaults = batch_definition_defaults or {}
        batch_definition_defaults_keys = set(batch_definition_defaults.keys())
        if not batch_definition_defaults_keys <= self.recognized_batch_definition_keys:
            logger.warning(
                "Unrecognized batch_definition key(s): %s"
                % str(
                    batch_definition_defaults_keys
                    - self.recognized_batch_definition_keys
                )
            )

        self._batch_definition_defaults = {
            key: value
            for key, value in batch_definition_defaults.items()
            if key in self.recognized_batch_definition_keys
        }

        self._partitioners = partitioners or {}
        self._default_partitioner = default_partitioner
        self._assets = assets
        self._config_params = config_params

        self._partitioners_cache: dict = {}
        self._partitions_cache: dict = {}

        self._execution_engine = execution_engine
        self._data_context_root_directory = data_context_root_directory

    @property
    def name(self) -> str:
        return self._name

    @property
    def partitioners(self) -> dict:
        return self._partitioners

    @property
    def default_partitioner(self) -> str:
        return self._default_partitioner

    @property
    def assets(self) -> dict:
        return self._assets

    @property
    def config_params(self) -> dict:
        return self._config_params

    @property
    def batch_definition_defaults(self) -> dict:
        return self._batch_definition_defaults

    @property
    def partitions_cache(self) -> dict:
        return self._partitions_cache

    def get_cached_partitions(self, data_asset_name: str = None) -> List[Partition]:
        if data_asset_name is None:
            return list(
                        itertools.chain.from_iterable(
                            [
                                partitions for name, partitions in self.partitions_cache.items()
                            ]
                        )
                    )
        return self.partitions_cache.get(data_asset_name)

    def update_partitions_cache(
        self,
        partitions: List[Partition],
        partitioner: Partitioner,
        allow_multipart_partitions: bool = False
    ):
        if not allow_multipart_partitions and partitions and len(partitions) > len(set(partitions)):
            raise ge_exceptions.PartitionerError(
                f'''Partitioner "{partitioner.name}" detected multiple data references in one or more partitions for the
given data asset; however, allow_multipart_partitions is set to False.  Please consider modifying the directives, used
to partition your dataset, or set allow_multipart_partitions to True, but be aware that unless you have a specific use
case for multipart partitions, there is most likely a mismatch between the partitioning directives and the actual
structure of data under consideration.
                '''
            )
        for partition in partitions:
            data_asset_name: str = partition.data_asset_name
            cached_partitions: List[Partition] = self.get_cached_partitions(
                data_asset_name=data_asset_name
            )
            if cached_partitions is None or len(cached_partitions) == 0:
                cached_partitions = []
            if partition in cached_partitions:
                identical_partitions: List[Partition] = [
                    temp_partition for temp_partition in cached_partitions if temp_partition == partition
                ]
                if not allow_multipart_partitions and len(identical_partitions) > 1:
                    raise ge_exceptions.PartitionerError(
                        f'''Partitioner "{partitioner.name}" detected multiple data references for partition
"{partition}" of data asset "{partition.data_asset_name}"; however, allow_multipart_partitions is set to
False.  Please consider modifying the directives, used to partition your dataset, or set allow_multipart_partitions to
True, but be aware that unless you have a specific use case for multipart partitions, there is most likely a mismatch
between the partitioning directives and the actual structure of data under consideration.
                        '''
                    )
                specific_partition_idx: int = cached_partitions.index(partition)
                specific_partition: Partition = cached_partitions[specific_partition_idx]
                if partition.data_reference != specific_partition.data_reference:
                    cached_partitions.remove(specific_partition)
                    cached_partitions.append(partition)
            else:
                partitions_with_given_data_reference: List[Partition] = [
                    temp_partition for temp_partition in cached_partitions if temp_partition.data_reference == partition.data_reference
                ]
                if len(partitions_with_given_data_reference) > 0:
                    raise ge_exceptions.PartitionerError(
                        f'''Partitioner "{partitioner.name}" for data asset "{partition.data_asset_name}" detected
multiple partitions, including "{partition}", for the same data reference -- this is illegal.
                        '''
                    )
                cached_partitions.append(partition)
            self._partitions_cache[data_asset_name] = cached_partitions

    def reset_partitions_cache(self, data_asset_name: str = None):
        if data_asset_name is None:
            self._partitions_cache = {}
        else:
            if data_asset_name in self.partitions_cache:
                self._partitions_cache[data_asset_name] = []

    def get_partitioner(self, name: str):
        """Get the (named) Partitioner from a DataConnector)

        Args:
            name (str): name of Partitioner

        Returns:
            Partitioner (Partitioner)
        """
        if name in self._partitioners_cache:
            return self._partitioners_cache[name]
        elif name in self.partitioners:
            partitioner_config: dict = copy.deepcopy(
                self.partitioners[name]
            )
        else:
            raise ge_exceptions.PartitionerError(
                f'Unable to load partitioner "{name}" -- no configuration found or invalid configuration.'
            )
        partitioner_config: CommentedMap = partitionerConfigSchema.load(
            partitioner_config
        )
        partitioner: Partitioner = self._build_partitioner_from_config(
            name=name, config=partitioner_config
        )
        self._partitioners_cache[name] = partitioner
        return partitioner

    def _build_partitioner_from_config(self, name: str, config: CommentedMap):
        """Build a Partitioner using the provided configuration and return the newly-built Partitioner."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, PartitionerConfig):
            config: dict = partitionerConfigSchema.dump(config)
        config.update({"name": name})
        partitioner: Partitioner = instantiate_class_from_config(
            config=config,
            runtime_environment={"data_connector": self},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.partitioner"
            },
        )
        if not partitioner:
            raise ge_exceptions.ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.partitioner",
                package_name=None,
                class_name=config["class_name"],
            )
        return partitioner

    def get_partitioner_for_data_asset(self, data_asset_name: str = None) -> Partitioner:
        partitioner_name: str
        data_asset_config_exists: bool = data_asset_name and self.assets and self.assets.get(data_asset_name)
        if data_asset_config_exists and self.assets[data_asset_name].get("partitioner"):
            partitioner_name = self.assets[data_asset_name]["partitioner"]
        else:
            partitioner_name = self.default_partitioner
        partitioner: Partitioner
        if partitioner_name is None:
            partitioner = NoOpPartitioner(
                name="NoOpPartitioner",
                data_connector=self,
                sorters=None,
                allow_multipart_partitions=False,
                config_params=None,
                module_name="great_expectations.execution_environment.data_connector.partitioner",
                class_name="NoOpPartitioner",
            )
        else:
            partitioner = self.get_partitioner(name=partitioner_name)
        return partitioner

    def build_batch_spec(self, batch_definition: dict) -> BatchSpec:
        if "data_asset_name" not in batch_definition:
            raise ge_exceptions.BatchSpecError("Batch definition must have a data_asset_name.")

        batch_definition_keys: set = set(batch_definition.keys())
        if not batch_definition_keys <= self.recognized_batch_definition_keys:
            logger.warning(
                "Unrecognized batch_definition key(s): %s"
                % str(batch_definition_keys - self.recognized_batch_definition_keys)
            )

        batch_definition_defaults: dict = copy.deepcopy(self.batch_definition_defaults)
        batch_definition: dict = {
            key: value
            for key, value in batch_definition.items()
            if key in self.recognized_batch_definition_keys
        }
        batch_definition: dict = nested_update(batch_definition_defaults, batch_definition)

        batch_spec_defaults: dict = copy.deepcopy(
            self._execution_engine.batch_spec_defaults
        )
        batch_spec_passthrough: dict = batch_definition.get("batch_spec_passthrough", {})
        batch_spec_scaffold: dict = nested_update(batch_spec_defaults, batch_spec_passthrough)

        data_asset_name: str = batch_definition.get("data_asset_name")
        batch_spec_scaffold["data_asset_name"] = data_asset_name

        partition_query: dict = batch_definition.get("partition_query")
        partitions: List[Partition] = self.get_available_partitions(
            data_asset_name=data_asset_name,
            partition_query=partition_query
        )
        if len(partitions) == 0:
            raise ge_exceptions.BatchSpecError(
                message=f'Unable to build batch_spec for data asset "{data_asset_name}".'
            )

        batch_spec: BatchSpec = self.build_batch_spec_from_partitions(
            partitions=partitions, batch_definition=batch_definition, batch_spec=batch_spec_scaffold
        )

        return batch_spec

    def build_batch_spec_from_partitions(
        self,
        partitions: List[Partition],
        batch_definition: dict,
        batch_spec: dict
    ) -> BatchSpec:
        raise NotImplementedError

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_partitions(
        self,
        data_asset_name: str = None,
        partition_query: Union[Dict[str, Union[int, list, tuple, slice, str, Dict, Callable, None]], None] = None,
        repartition: bool = False
    ) -> List[Partition]:
        partitioner: Partitioner = self.get_partitioner_for_data_asset(data_asset_name=data_asset_name)
        if partition_query is None:
            partition_query = {}
        partition_query["data_asset_name"] = data_asset_name
        partition_query_obj: PartitionQuery = build_partition_query(partition_query_dict=partition_query)
        return self._get_available_partitions(
            partitioner=partitioner,
            data_asset_name=data_asset_name,
            partition_query=partition_query_obj,
            repartition=repartition
        )

    def _get_available_partitions(
        self,
        partitioner: Partitioner,
        data_asset_name: str = None,
        partition_query: Union[PartitionQuery, None] = None,
        repartition: bool = False
    ) -> List[Partition]:
        raise NotImplementedError
