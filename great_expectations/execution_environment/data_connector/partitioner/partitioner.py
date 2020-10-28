# # TODO: <Alex>The Partitioner class is being decommissioned.  Comment it out, update tests, then delete deprecated/unused code.</Alex>
# # -*- coding: utf-8 -*-
#
# import copy
# from typing import Union, List, Iterator, Any
#
# import logging
#
# from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
# from great_expectations.execution_environment.data_connector.sorter import Sorter
# from great_expectations.core.batch import BatchRequest
# import great_expectations.exceptions as ge_exceptions
#
# from great_expectations.data_context.util import (
#     instantiate_class_from_config,
# )
#
# logger = logging.getLogger(__name__)
#
#
# class Partitioner(object):
#     DEFAULT_DELIMITER: str = "-"
#
#     def __init__(
#         self,
#         name: str,
#         sorters: list = None,
#         allow_multipart_partitions: bool = False,
#         runtime_keys: list = None
#     ):
#         self._name = name
#         self._sorters = sorters
#         self._allow_multipart_partitions = allow_multipart_partitions
#         self._runtime_keys = runtime_keys
#         self._sorters_cache = {}
#
#     @property
#     def name(self) -> str:
#         return self._name
#
#     @property
#     def sorters(self) -> Union[List[Sorter], None]:
#         if self._sorters:
#             return [self.get_sorter(name=sorter_config["name"]) for sorter_config in self._sorters]
#         return None
#
#     @property
#     def allow_multipart_partitions(self) -> bool:
#         return self._allow_multipart_partitions
#
#     @property
#     def runtime_keys(self) -> list:
#         return self._runtime_keys
#
#     def get_sorter(self, name) -> Sorter:
#         """Get the (named) Sorter from a Partitioner)
#
#         Args:
#             name (str): name of Sorter
#
#         Returns:
#             Sorter (Sorter)
#         """
#         if name in self._sorters_cache:
#             return self._sorters_cache[name]
#         else:
#             if self._sorters:
#                 sorter_names: list = [sorter_config["name"] for sorter_config in self._sorters]
#                 if name in sorter_names:
#                     sorter_config: dict = copy.deepcopy(
#                         self._sorters[sorter_names.index(name)]
#                     )
#                 else:
#                     raise ge_exceptions.SorterError(
#                         f'''Unable to load sorter with the name "{name}" -- no configuration found or invalid
# configuration.
#                         '''
#                     )
#             else:
#                 raise ge_exceptions.SorterError(
#                     f'Unable to load sorter with the name "{name}" -- no configuration found or invalid configuration.'
#                 )
#         sorter: Sorter = self._build_sorter_from_config(
#             name=name, config=sorter_config
#         )
#         self._sorters_cache[name] = sorter
#         return sorter
#
#     @staticmethod
#     def _build_sorter_from_config(name: str, config: dict) -> Sorter:
#         """Build a Sorter using the provided configuration and return the newly-built Sorter."""
#         runtime_environment: dict = {
#             "name": name
#         }
#         sorter: Sorter = instantiate_class_from_config(
#             config=config,
#             runtime_environment=runtime_environment,
#             config_defaults={
#                 "module_name": "great_expectations.execution_environment.data_connector.sorter"
#             },
#         )
#         if not sorter:
#             raise ge_exceptions.ClassInstantiationError(
#                 module_name="great_expectations.execution_environment.data_connector.sorter",
#                 package_name=None,
#                 class_name=config["class_name"],
#             )
#         return sorter
#
#     # TODO: <Alex>Should this be a private method?</Alex>
#     def get_sorted_partitions(self, partitions: List[Partition]) -> List[Partition]:
#         if self.sorters and len(self.sorters) > 0:
#             sorters: Iterator[Sorter] = reversed(self.sorters)
#             for sorter in sorters:
#                 partitions = sorter.get_sorted_partitions(partitions=partitions)
#             return partitions
#         return partitions
#
#     def convert_batch_request_to_data_reference(
#         self,
#         batch_request: BatchRequest
#     ) -> Any:
#         raise NotImplementedError
#
#     def convert_data_reference_to_batch_request(
#         self,
#         data_reference: Any
#     ) -> BatchRequest:
#         raise NotImplementedError
#
#     def _compute_partitions_for_data_asset(
#         self,
#         data_asset_name: str = None,
#         runtime_parameters: Union[dict, None] = None,
#         **kwargs
#     ) -> List[Partition]:
#         raise NotImplementedError
#
#     def _validate_sorters_configuration(self, partition_keys: List[str], num_actual_partition_keys: int):
#         if self.sorters and len(self.sorters) > 0:
#             if any([sorter.name not in partition_keys for sorter in self.sorters]):
#                 raise ge_exceptions.PartitionerError(
#                     f'''Partitioner "{self.name}" specifies one or more sort keys that do not appear among the
# configured partition keys.
#                     '''
#                 )
#             if len(partition_keys) < len(self.sorters):
#                 raise ge_exceptions.PartitionerError(
#                     f'''Partitioner "{self.name}", configured with {len(partition_keys)} partition keys, matches
# {num_actual_partition_keys} actual partition keys; this is fewer than number of sorters specified, which is
# {len(self.sorters)}.
#                     '''
#                 )
#
#     def _validate_runtime_keys_configuration(self, runtime_keys: List[str]):
#         if runtime_keys and len(runtime_keys) > 0:
#             if not (self.runtime_keys and set(runtime_keys) <= set(self.runtime_keys)):
#                 raise ge_exceptions.PartitionerError(
#                     f'''Partitioner "{self.name}" was invoked with one or more runtime keys that do not appear among the
# configured runtime keys.
#                     '''
#                 )
