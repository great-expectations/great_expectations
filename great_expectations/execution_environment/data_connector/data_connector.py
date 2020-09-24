# -*- coding: utf-8 -*-

import copy
import logging
import itertools
from typing import List, Iterator
# TODO: <Alex>Do we need warnings?</Alex>
import warnings
from copy import deepcopy

from great_expectations.data_context.types.base import (
    PartitionerConfig,
    partitionerConfigSchema
)
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import Sorter
from great_expectations.core.id_dict import BatchSpec
from great_expectations.core.util import nested_update
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError

logger = logging.getLogger(__name__)


class DataConnector(object):
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

    _default_reader_options = {}
    # TODO: <Alex>Is this needed?</Alex>
    _batch_spec_type = BatchSpec
    # TODO: <Alex>Check these carefully -- remove the wrong ones.</Alex>
    recognized_batch_definition_keys = {
        "execution_environment",
        "data_connector",
        "data_asset_name",
        "partition_name",
        "batch_spec_passthrough",
        "limit",
    }

    # TODO: <Alex>Add type hints throughout</Alex>
    def __init__(
        self,
        name,
        execution_environment,
        partitioners=None,
        default_partitioner=None,
        assets=None,
        batch_definition_defaults=None,
        **kwargs
    ):
        self._name = name

        # TODO: <Alex></Alex>
        self._data_connector_config = kwargs

        # TODO: <Alex>Is this needed?</Alex>
        self._data_asset_iterators = {}

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
        if execution_environment is None:
            raise ValueError(
                "execution environment must be provided for a DataConnector"
            )

        self._execution_environment = execution_environment
        self._partitioners = partitioners or {}
        self._default_partitioner = default_partitioner
        self._assets = assets

        self._partitioners_cache: dict = {}
        self._partitions_cache: dict = {}

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
        return self._data_connector_config.get("config_params")

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

    def update_partitions_cache(self, partitions: List[Partition]):
        for partition in partitions:
            data_asset_name: str = partition.data_asset_name
            cached_partitions: List[Partition] = self.get_cached_partitions(
                data_asset_name=data_asset_name
            )
            if cached_partitions is None or len(cached_partitions) == 0:
                cached_partitions = []
            if partition not in cached_partitions:
                cached_partitions.append(partition)
            self._partitions_cache[data_asset_name] = cached_partitions

    def get_partitioner(self, name):
        """Get the (named) Partitioner from a DataConnector)

        Args:
            name (str): name of Partitioner

        Returns:
            Partitioner (Partitioner)
        """
        if name in self._partitioners_cache:
            return self._partitioners_cache[name]
        elif name in self.partitioners:
            partitioner_config = copy.deepcopy(
                self.partitioners[name]
            )
        else:
            raise ValueError(
                "Unable to load partitioner %s -- no configuration found or invalid configuration."
                % name
            )
        partitioner_config = partitionerConfigSchema.load(
            partitioner_config
        )
        partitioner = self._build_partitioner_from_config(
            name=name, config=partitioner_config
        )
        self._partitioners_cache[name] = partitioner
        return partitioner

    # TODO: <Alex>This is a good place to check that all defaults from base.py / Config Schemas are set properly.</Alex>
    def _build_partitioner_from_config(self, name, config):
        """Build a Partitioner using the provided configuration and return the newly-built Partitioner."""
        # We convert from the type back to a dictionary for purposes of instantiation
        if isinstance(config, PartitionerConfig):
            config = partitionerConfigSchema.dump(config)
        config.update({"name": name})
        partitioner = instantiate_class_from_config(
            config=config,
            runtime_environment={"data_connector": self},
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.partitioner"
            },
        )
        if not partitioner:
            raise ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.partitioner",
                package_name=None,
                class_name=config["class_name"],
            )
        return partitioner

    def get_available_data_asset_names(self):
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_partitions(self, partition_name: str = None, data_asset_name: str = None) -> List[Partition]:
        raise NotImplementedError

    # TODO: <Alex>This method is not useful -- get_available_partitions provides the complete information.</Alex>
    # def get_available_partition_names(self, data_asset_name: str = None) -> List[str]:
    #     return [
    #         partition.name for partition in self.get_available_partitions(
    #             partition_name=None,
    #             data_asset_name=data_asset_name
    #         )
    #     ]

    def get_config(self):
        # TODO: <Alex>Do we want to make ExecutionEnvironment._execution_environment_config["data_connectors"] or some convenience method publicly accessible to avoid PyCharm warnings?</Alex>
        conf: dict = self._execution_environment._execution_environment_config["data_connectors"][self.name]
        conf.update(self._data_connector_config)
        return conf

    # TODO: <Alex>Without source (e.g., path) specified in batch_spec, ExecutionEngine cannot use batch_spec to load data; hence, should we discontinue the iterator / next batch_spec usecase?</Alex>
    # def get_iterator(self, data_asset_name=None, **kwargs):
    #     if not data_asset_name:
    #         raise ValueError("Please provide data_asset_name.")
    #
    #     if data_asset_name in self._data_asset_iterators:
    #         data_asset_iterator, passed_kwargs = self._data_asset_iterators[
    #             data_asset_name
    #         ]
    #         if passed_kwargs != kwargs:
    #             logger.warning(
    #                 "Asked to yield batch_spec using different supplemental kwargs. Please reset iterator to "
    #                 "use different supplemental kwargs."
    #             )
    #         return data_asset_iterator
    #     else:
    #         self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
    #         return self._data_asset_iterators[data_asset_name][0]
    #
    # def yield_batch_spec(self, data_asset_name, batch_definition, batch_spec):
    #     if data_asset_name not in self._data_asset_iterators:
    #         self.reset_iterator(
    #             data_asset_name=data_asset_name,
    #             batch_definition=batch_definition,
    #             batch_spec=batch_spec,
    #         )
    #     data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
    #         data_asset_name
    #     ]
    #     if passed_batch_definition != batch_definition:
    #         logger.warning(
    #             "Asked to yield batch_spec using different supplemental batch_definition. Resetting iterator to "
    #             "use new supplemental batch_definition."
    #         )
    #         self.reset_iterator(
    #             data_asset_name=data_asset_name,
    #             batch_definition=batch_definition,
    #             batch_spec=batch_spec,
    #         )
    #         data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
    #             data_asset_name
    #         ]
    #     try:
    #         batch_spec = next(data_asset_iterator)
    #         return batch_spec
    #     except StopIteration:
    #         self.reset_iterator(
    #             data_asset_name=data_asset_name,
    #             batch_definition=batch_definition,
    #             batch_spec=batch_spec,
    #         )
    #         data_asset_iterator, passed_batch_definition = self._data_asset_iterators[
    #             data_asset_name
    #         ]
    #         if passed_batch_definition != batch_definition:
    #             logger.warning(
    #                 "Asked to yield batch_spec using different batch parameters. Resetting iterator to "
    #                 "use different batch parameters."
    #             )
    #             self.reset_iterator(
    #                 data_asset_name=data_asset_name,
    #                 batch_definition=batch_definition,
    #                 batch_spec=batch_spec,
    #             )
    #             data_asset_iterator, passed_batch_definition = self._data_asset_iterators[data_asset_name]
    #         try:
    #             batch_spec = next(data_asset_iterator)
    #             return batch_spec
    #         except StopIteration:
    #             # This is a degenerate case in which no batch_definition are actually being generated
    #             logger.warning(
    #                 "No batch_spec found for data_asset_name %s" % data_asset_name
    #             )
    #             return {}
    #     except TypeError:
    #         # If we do not actually have an iterator we can generate, even after resetting, then just return empty dict.
    #         logger.warning(
    #             "Unable to generate batch_spec for data_asset_name %s" % data_asset_name
    #         )
    #         return {}
    #
    # def reset_iterator(self, data_asset_name, batch_definition, batch_spec):
    #     self._data_asset_iterators[data_asset_name] = (
    #         self._get_iterator(
    #             data_asset_name=data_asset_name,
    #             batch_definition=batch_definition,
    #             batch_spec=batch_spec,
    #         ),
    #         batch_definition,
    #     )
    #
    # def _get_iterator(self, data_asset_name, batch_definition, batch_spec):
    #     raise NotImplementedError

    def build_batch_spec(self, batch_definition):
        if "data_asset_name" not in batch_definition:
            raise ValueError("Batch definition must have a data_asset_name.")

        batch_definition_keys = set(batch_definition.keys())
        recognized_batch_definition_keys = (
            self.recognized_batch_definition_keys
            | self._execution_environment.execution_engine.recognized_batch_definition_keys
        )
        if not batch_definition_keys <= recognized_batch_definition_keys:
            logger.warning(
                "Unrecognized batch_definition key(s): %s"
                % str(batch_definition_keys - recognized_batch_definition_keys)
            )

        batch_definition_defaults = deepcopy(self.batch_definition_defaults)
        batch_definition = {
            key: value
            for key, value in batch_definition.items()
            if key in recognized_batch_definition_keys
        }
        batch_definition = nested_update(batch_definition_defaults, batch_definition)

        batch_spec_defaults = deepcopy(
            self._execution_environment.execution_engine.batch_spec_defaults
        )
        batch_spec_passthrough = batch_definition.get("batch_spec_passthrough", {})
        batch_spec_scaffold = nested_update(batch_spec_defaults, batch_spec_passthrough)

        batch_spec_scaffold["data_asset_name"] = batch_definition.get("data_asset_name")

        batch_spec_scaffold["execution_environment"] = self._execution_environment.name

        batch_spec = self._build_batch_spec(
            batch_definition=batch_definition, batch_spec=batch_spec_scaffold
        )

        return batch_spec

    # TODO: will need to handle partition_definition for in-memory df case
    def _build_batch_spec(self, batch_definition, batch_spec):
        return BatchSpec(batch_spec)

