# -*- coding: utf-8 -*-

import copy
import itertools
from typing import List, Dict, Union, Callable, Any, Tuple
from ruamel.yaml.comments import CommentedMap
import json

import logging

from great_expectations.data_context.types.base import (
    PartitionerConfig,
    partitionerConfigSchema
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.partition_request import (
    PartitionRequest,
    build_partition_request
)
from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinitionSubset,
    PartitionDefinition,
    BatchSpec,
)
from great_expectations.core.batch import (
    BatchMarkers,
    BatchDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class DataConnector(object):
    """
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
    DEFAULT_DATA_ASSET_NAME: str = "DEFAULT_DATA_ASSET"

    _default_reader_options: dict = {}

    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        assets: dict = None,
        partitioners: dict = None,
        default_partitioner_name: str = None,
        execution_engine: ExecutionEngine = None,
        data_context_root_directory: str = None
    ):
        self._name = name

        self.execution_environment_name = execution_environment_name

        if assets is None:
            assets = {}
        _assets: Dict[str, Union[dict, Asset]] = assets
        self._assets = _assets
        self._build_assets_from_config(config=assets)

        if partitioners is None:
            partitioners = {}
        _partitioners: Dict[str, Union[dict, Partitioner]] = partitioners
        self._partitioners = _partitioners
        self._build_partitioners_from_config(config=partitioners)

        self._default_partitioner_name = default_partitioner_name

        self._execution_engine = execution_engine

        self._data_context_root_directory = data_context_root_directory

        # TODO: <Alex>The next 2 lines should be deleted once the user of the Partion object has been deprecated.</Alex>
        # The partitions cache is a dictionary, which maintains lists of partitions for a data_asset_name as the key.
        # self._partitions_cache: dict = {}

        # This is a dictionary which maps data_references onto batch_requests
        self._data_references_cache = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def assets(self) -> Dict[str, Union[dict, Asset]]:
        return self._assets

    @property
    def partitioners(self) -> Dict[str, Union[dict, Partitioner]]:
        return self._partitioners

    @property
    def default_partitioner(self) -> Union[str, Partitioner]:
        try:
            return self.partitioners[self._default_partitioner_name]
        except KeyError:
            raise ValueError("No default partitioner has been set")

#     def _get_cached_partitions(
#         self,
#         data_asset_name: str = None,
#         runtime_parameters: Union[PartitionDefinitionSubset, None] = None
#     ) -> List[Partition]:
#         cached_partitions: List[Partition]
#         if data_asset_name is None:
#             cached_partitions = list(
#                 itertools.chain.from_iterable(
#                     [
#                         partitions for name, partitions in self._partitions_cache.items()
#                     ]
#                 )
#             )
#         else:
#             cached_partitions = self._partitions_cache.get(data_asset_name)
#         if runtime_parameters is None:
#             return cached_partitions
#         else:
#             if not cached_partitions:
#                 return []
#             return list(
#                 filter(
#                     lambda partition: self._cache_partition_runtime_parameters_filter(
#                         partition=partition,
#                         parameters=runtime_parameters
#                     ),
#                     cached_partitions
#                 )
#             )

#     @staticmethod
#     def _cache_partition_runtime_parameters_filter(partition: Partition, parameters: PartitionDefinitionSubset) -> bool:
#         partition_definition: PartitionDefinition = partition.definition
#         for key, value in parameters.items():
#             if not (key in partition_definition and partition_definition[key] == value):
#                 return False
#         return True

#     def update_partitions_cache(
#         self,
#         partitions: List[Partition],
#         partitioner_name: str,
#         runtime_parameters: PartitionDefinition,
#         allow_multipart_partitions: bool = False
#     ):
#         """
#         The cache of partitions (keyed by a data_asset_name) is identified by the combination of the following entities:
#         -- name of the partition (string)
#         -- data_asset_name (string)
#         -- partition definition (dictionary)
#         Configurably, these entities are either supplied by the user function or computed by the specific partitioner.

#         In order to serve as the identity of the Partition object, the above fields are hashed.  Hence, Partition
#         objects can be utilized in set operations, tested for presence in lists, containing multiple Partition objects,
#         and participate in any other logic operations, where equality checks play a role.  This is particularly
#         important, because one-to-many mappings (multiple "same" partition objects, differing only by the data_reference
#         property) especially if referencing different data objects, are illegal (unless configured otherwise).

#         In addition, it is considered illegal to have the same data_reference property in multiple Partition objects.

#         Note that the data_reference field of the Partition object does not participate in identifying a partition.
#         The reason for this is that data references can vary in type (files on a filesystem, S3 objects, Pandas or Spark
#         DataFrame objects, etc.) and maintaining references to them in general can result in large memory consumption.
#         Moreover, in the case of Pandas and Spark DataFrame objects (and other in-memory datasets), the metadata aspects
#         of the data object (as captured by the partition information) may remain the same, while the actual dataset
#         changes (e.g., the output of a processing stage of a data pipeline).  In such situations, if a new partition
#         is found to be identical to an existing partition, the data_reference property of the new partition is accepted.
#         """

#         ### <WILL> THIS IS WHERE THE LOGIC WILL BE PULLED OUT FROM
#         # Prevent non-unique partitions in submitted list of partitions.
#         if not allow_multipart_partitions and partitions and len(partitions) > len(set(partitions)):
#             raise ge_exceptions.PartitionerError(
#                 f'''Partitioner "{partitioner_name}" detected multiple data references in one or more partitions for the
# given data asset; however, allow_multipart_partitions is set to False.  Please consider modifying the directives, used
# to partition your dataset, or set allow_multipart_partitions to True, but be aware that unless you have a specific use
# case for multipart partitions, there is most likely a mismatch between the partitioning directives and the actual
# structure of data under consideration.
#                 '''
#             )
#         for partition in partitions:
#             data_asset_name: str = partition.data_asset_name
#             cached_partitions: List[Partition] = self._get_cached_partitions(
#                 data_asset_name=data_asset_name,
#                 runtime_parameters=runtime_parameters
#             )
#             if cached_partitions is None or len(cached_partitions) == 0:
#                 cached_partitions = []
#             if partition in cached_partitions:
#                 # Prevent non-unique partitions in anticipated list of partitions.
#                 non_unique_partitions: List[Partition] = [
#                     temp_partition
#                     for temp_partition in cached_partitions
#                     if temp_partition == partition
#                 ]
#                 if not allow_multipart_partitions and len(non_unique_partitions) > 1:
#                     raise ge_exceptions.PartitionerError(
#                         f'''Partitioner "{partitioner_name}" detected multiple data references for partition
# "{partition}" of data asset "{partition.data_asset_name}"; however, allow_multipart_partitions is set to
# False.  Please consider modifying the directives, used to partition your dataset, or set allow_multipart_partitions to
# True, but be aware that unless you have a specific use case for multipart partitions, there is most likely a mismatch
# between the partitioning directives and the actual structure of data under consideration.
#                         '''
#                     )
#                 # Attempt to update the data_reference property with that provided as part of the submitted partition.
#                 specific_partition_idx: int = cached_partitions.index(partition)
#                 specific_partition: Partition = cached_partitions[specific_partition_idx]
#                 if specific_partition.data_reference != partition.data_reference:
#                     specific_partition.data_reference = partition.data_reference
#             else:
#                 # Prevent the same data_reference property value to be represented by multiple partitions.
#                 partitions_with_given_data_reference: List[Partition] = [
#                     temp_partition
#                     for temp_partition in cached_partitions
#                     if temp_partition.data_reference == partition.data_reference
#                 ]
#                 if len(partitions_with_given_data_reference) > 0:
#                     raise ge_exceptions.PartitionerError(
#                         f'''Partitioner "{partitioner_name}" for data asset "{partition.data_asset_name}" detected
# multiple partitions, including "{partition}", for the same data reference -- this is illegal.
#                         '''
#                     )
#                 cached_partitions.append(partition)
#             self._partitions_cache[data_asset_name] = cached_partitions

#     def reset_partitions_cache(self, data_asset_name: str = None):
#         if data_asset_name is None:
#             self._partitions_cache = {}
#         else:
#             if data_asset_name in self._partitions_cache:
#                 self._partitions_cache[data_asset_name] = []

    # def get_partitioner(self, name: str):
    #     """Get the (named) Partitioner from a DataConnector)

    #     Args:
    #         name (str): name of Partitioner

    #     Returns:
    #         Partitioner (Partitioner)
    #     """
    #     if name in self._partitioners_cache:
    #         return self._partitioners_cache[name]
    #     elif name in self.partitioners:
    #         partitioner_config: dict = copy.deepcopy(
    #             self.partitioners[name]
    #         )
    #     else:
    #         raise ge_exceptions.PartitionerError(
    #             f'Unable to load partitioner "{name}" -- no configuration found or invalid configuration.'
    #         )
    #     partitioner_config: CommentedMap = partitionerConfigSchema.load(
    #         partitioner_config
    #     )
    #     partitioner: Partitioner = self._build_partitioner_from_config(
    #         name=name, config=partitioner_config
    #     )
    #     self._partitioners_cache[name] = partitioner
    #     return partitioner

    # TODO: <Alex>We should not need this method any more; hence, it should be deleted.</Alex>
    # def add_partitioner(self, partitioner_name: str, partitioner_config: dict) -> Partitioner:
    #     """Add a new Partitioner to the DataConnector and (for convenience) return the instantiated Partitioner object.
    #
    #     Args:
    #         partitioner_name (str): a key for the new Store in in self._stores
    #         partitioner_config (dict): a config for the Store to add
    #
    #     Returns:
    #         partitioner (Partitioner)
    #     """
    #
    #     new_partitioner = self._build_partitioner_from_config(partitioner_name, partitioner_config)
    #     self.partitioners[partitioner_name] = new_partitioner
    #
    #     return new_partitioner

    # Replaces the asset configuration with the corresponding Asset object in the assets dictionary.
    def _build_assets_from_config(self, config: Dict[str, dict]):
        for name, asset_config in config.items():
            if asset_config is None:
                raise ValueError("Asset config should not be None.")
            new_asset: Asset = self._build_asset_from_config(
                name=name,
                config=asset_config,
            )
            self.assets[name] = new_asset

    def _build_asset_from_config(self, name: str, config: dict):
        """Build an Asset using the provided configuration and return the newly-built Asset."""
        runtime_environment: dict = {
            "name": name,
            "data_connector": self
        }
        asset: Asset = instantiate_class_from_config(
            config=config,
            runtime_environment=runtime_environment,
            config_defaults={
                "module_name": "great_expectations.execution_environment.data_connector.asset",
                "class_name": "Asset"
            },
        )
        if not asset:
            raise ge_exceptions.ClassInstantiationError(
                module_name="great_expectations.execution_environment.data_connector.asset",
                package_name=None,
                class_name=config["class_name"],
            )
        return asset

    # Replaces the partitoiner configuration with the corresponding Partitioner object in the partitioners dictionary.
    def _build_partitioners_from_config(self, config: Dict[str, dict]):
        for name, partitioner_config in config.items():
            new_partitioner = self._build_partitioner_from_config(
                name,
                partitioner_config,
            )
            self.partitioners[name] = new_partitioner

    def _build_partitioner_from_config(self, name: str, config: dict):
        """Build a Partitioner using the provided configuration and return the newly-built Partitioner."""
        runtime_environment: dict = {
            "name": name,
            "data_connector": self
        }
        partitioner: Partitioner = instantiate_class_from_config(
            config=config,
            runtime_environment=runtime_environment,
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
        # data_asset_config_exists: bool = data_asset_name and self.assets and self.assets.get(data_asset_name)
        data_asset_config_exists: bool = data_asset_name is not None and isinstance(
            self.assets.get(data_asset_name), Asset
        )
        if data_asset_config_exists and self.assets[data_asset_name].partitioner_name:
            partitioner_name = self.assets[data_asset_name].partitioner_name
        else:
            partitioner_name = self.default_partitioner.name
        partitioner: Partitioner
        if partitioner_name is None:
            raise ge_exceptions.BatchSpecError(
                message=f'''
No partitioners found for data connector "{self.name}" -- at least one partitioner must be configured for a data
connector and the default_partitioner_name is set to the name of one of the configured partitioners.
                '''
            )
        else:
            partitioner = self.partitioners[partitioner_name]
        return partitioner

    # def _build_batch_spec(self, batch_request: BatchRequest, partition: Partition) -> BatchSpec:
    #     if not batch_request.data_asset_name:
    #         raise ge_exceptions.BatchSpecError("Batch request must have a data_asset_name.")

    #     batch_spec_scaffold: BatchSpec
    #     batch_spec_passthrough: BatchSpec = batch_request.batch_spec_passthrough
    #     if batch_spec_passthrough is None:
    #         batch_spec_scaffold = BatchSpec()
    #     else:
    #         batch_spec_scaffold = copy.deepcopy(batch_spec_passthrough)

    #     data_asset_name: str = batch_request.data_asset_name
    #     batch_spec_scaffold["data_asset_name"] = data_asset_name

    #     batch_spec: BatchSpec = self._build_batch_spec_from_partition(
    #         partition=partition, batch_request=batch_request, batch_spec=batch_spec_scaffold
    #     )

    #     return batch_spec

    # def _build_batch_spec_from_partition(
    #     self,
    #     partition: Partition,
    #     batch_request: BatchRequest,
    #     batch_spec: BatchSpec
    # ) -> BatchSpec:
    #     raise NotImplementedError

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_available_partitions(
        self,
        data_asset_name: str = None,
        batch_request: BatchRequest = None,
        partition_request: Union[
            Dict[str, Union[int, list, tuple, slice, str, Union[Dict, PartitionDefinitionSubset], Callable, None]], None
        ] = None,
        in_memory_dataset: Any = None,
        runtime_parameters: Union[dict, None] = None,
        repartition: bool = False
    ) -> List[Partition]:
        partitioner: Partitioner = self.get_partitioner_for_data_asset(data_asset_name=data_asset_name)
        partition_request_obj: PartitionRequest = build_partition_request(partition_request_dict=partition_request)
        if runtime_parameters is not None:
            runtime_parameters: PartitionDefinitionSubset = PartitionDefinitionSubset(runtime_parameters)
        return self._get_available_partitions(
            partitioner=partitioner,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            partition_request=partition_request_obj,
            in_memory_dataset=in_memory_dataset,
            runtime_parameters=runtime_parameters,
            repartition=repartition
        )

    def _get_available_partitions(
        self,
        partitioner: Partitioner,
        data_asset_name: str = None,
        batch_request: BatchRequest = None,
        partition_request: Union[PartitionRequest, None] = None,
        in_memory_dataset: Any = None,
        runtime_parameters: Union[PartitionDefinitionSubset, None] = None,
        repartition: bool = False
    ) -> List[Partition]:
        raise NotImplementedError

    def _batch_definition_matches_batch_request(
        self,
        batch_definition: BatchDefinition,
        batch_request: BatchRequest,
    ) -> bool:
        assert isinstance(batch_definition, BatchDefinition)
        assert isinstance(batch_request, BatchRequest)
        if batch_request.execution_environment_name:
            if batch_request.execution_environment_name != batch_definition.execution_environment_name:
                return False
        if batch_request.data_connector_name:
            if batch_request.data_connector_name != batch_definition.data_connector_name:
                return False
        if batch_request.data_asset_name:
            if batch_request.data_asset_name != batch_definition.data_asset_name:
                return False
        #FIXME: This is too rigid. Needs to take into account ranges and stuff.
        if batch_request.partition_request:
            for k,v in batch_request.partition_request.items():
                if (not k in batch_definition.partition_definition) or batch_definition.partition_definition[k] != v:
                    return False
        return True

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        if batch_request.data_connector_name != self.name:
            raise ValueError(f"data_connector_name {batch_request.data_connector_name} does not match name {self.name}.")

        if self._data_references_cache == None:
            self.refresh_data_references_cache()

        batches = []
        for data_reference, batch_definition in self._data_references_cache.items():
            if batch_definition == None:
                # The data_reference is unmatched.
                continue
            if self._batch_definition_matches_batch_request(batch_definition, batch_request):
                batches.append(batch_definition)

        return batches

    def get_batch_data_and_metadata_from_batch_definition(
        self,
        batch_definition: BatchDefinition,
    ) -> Tuple[
        Any, #batch_data
        BatchSpec,
        BatchMarkers,
    ]:
        batch_spec = self._build_batch_spec_from_batch_definition(batch_definition)
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            **batch_spec
        )

        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def _build_batch_spec_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> BatchSpec:
        batch_spec_params = self._generate_batch_spec_parameters_from_batch_definition(
            batch_definition
        )
        # TODO Abe 20201018: Decide if we want to allow batch_spec_passthrough parameters anywhere.
        batch_spec = BatchSpec(
            **batch_spec_params
        )

        return batch_spec

    def _generate_batch_spec_parameters_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> dict:
        raise NotImplementedError

    def _generate_partition_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest
    ) -> dict:
        #FIXME: switch this to use the data_reference cache instead of the partition cache.
        available_partitions = self.get_available_partitions(
            data_asset_name=batch_request.data_asset_name,
            batch_request=batch_request,
            partition_request=batch_request.partition_request
        )
        return [partition.definition for partition in available_partitions]

    def refresh_data_references_cache(
        self,
    ):
        """
        """
        #Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list = self._map_data_reference_to_batch_definition_list(
                data_reference,
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    def get_unmatched_data_references(self):
        if self._data_references_cache == None:
            raise ValueError("_data_references_cache is None. Have you called refresh_data_references_cache yet?")

        return [k for k,v in self._data_references_cache.items() if v == None]
    
    def get_data_reference_list_count(self):
        return len(self._data_references_cache)

    #TODO Abe 20201015: This method is still somewhat janky. Needs better supporting methods, plus more thought and hardening.
    def _map_data_reference_to_batch_definition_list(self,
        data_reference,
    ) -> List[BatchDefinition]:
    #FIXME: Make this smarter about choosing the right partitioner
        try:
            self.default_partitioner
        except ValueError:
            raise ge_exceptions.DataConnectorError("Default Partitioner has not been set for data_connector")

        batch_request = self.default_partitioner.convert_data_reference_to_batch_request(
            data_reference
        )
        if batch_request == None:
            return None
        if batch_request.data_asset_name:
            data_asset_name = batch_request.data_asset_name
        # process assets to populate data_asset_name in batch_definition:
        else:
            data_asset_name = "FAKE_DATA_ASSET_NAME"
        return BatchDefinition(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
            data_asset_name=data_asset_name,
            partition_definition=batch_request.partition_request,
        )

    def self_check(self,
        pretty_print=True,
        max_examples=3
    ):
        if self._data_references_cache == None:
            self.refresh_data_references_cache()

        if pretty_print:
            print("\t"+self.name, ":", self.__class__.__name__)
            print()

        asset_names = self.get_available_data_asset_names()
        asset_names.sort()
        len_asset_names = len(asset_names)

        data_connector_obj = {
            "class_name" : self.__class__.__name__,
            "data_asset_count" : len_asset_names,
            "example_data_asset_names": asset_names[:max_examples],
            "data_assets" : {}
            # "data_reference_count": self.
        }

        if pretty_print:
            print(f"\tAvailable data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):")
        
        for asset_name in asset_names[:max_examples]:
            batch_definition_list = self.get_batch_definition_list_from_batch_request(BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=asset_name,
            ))
            len_batch_definition_list = len(batch_definition_list)
            
            example_data_references = [
                self.default_partitioner.convert_batch_request_to_data_reference(BatchRequest(
                    execution_environment_name=batch_definition.execution_environment_name,
                    data_connector_name=batch_definition.data_connector_name,
                    data_asset_name=batch_definition.data_asset_name,
                    partition_request=batch_definition.partition_definition,
                ))
                for batch_definition in batch_definition_list
            ][:max_examples]
            example_data_references.sort()

            if pretty_print:
                print(f"\t\t{asset_name} ({min(len_batch_definition_list, max_examples)} of {len_batch_definition_list}):", example_data_references)

            data_connector_obj["data_assets"][asset_name] = {
                "batch_definition_count": len_batch_definition_list,
                "example_data_references": example_data_references
            }

        unmatched_data_references = self.get_unmatched_data_references()
        len_unmatched_data_references = len(unmatched_data_references)
        if pretty_print:
            print(f"\n\tUnmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}):", unmatched_data_references[:max_examples])
        
        data_connector_obj["unmatched_data_reference_count"] = len_unmatched_data_references
        data_connector_obj["example_unmatched_data_references"] = unmatched_data_references[:max_examples]

        return data_connector_obj
