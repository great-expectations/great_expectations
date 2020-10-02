# -*- coding: utf-8 -*-

import sys
from typing import List, Dict, Callable, Union

import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


def build_partition_query(
    partition_query_dict: Union[Dict[str, Union[int, str, Dict, Callable]], None] = None
):
    if not partition_query_dict:
        return PartitionQuery(
            custom_filter=None,
            partition_index=None,
            partition_name=None,
            partition_definition=None,
            data_asset_name=None,
            limit=None
        )
    partition_query_keys: set = set(partition_query_dict.keys())
    if not partition_query_keys <= PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS:
        raise ge_exceptions.PartitionerError(
            f'''Unrecognized partition_query key(s):
"{str(partition_query_keys - PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS)}" detected.
            '''
        )
    custom_filter: Callable = partition_query_dict.get("custom_filter")
    if custom_filter and not isinstance(custom_filter, Callable):
        raise ge_exceptions.PartitionerError(
            f'''The type of a custom_filter be a function (Python "Callable").  The type given is
"{str(type(custom_filter))}", which is illegal.
            '''
        )
    partition_index: int = partition_query_dict.get("partition_index")
    if partition_index and not isinstance(partition_index, int):
        raise ge_exceptions.PartitionerError(
            f'''The type of a partition_index must be an integer (Python "int").  The type given is
"{str(type(partition_index))}", which is illegal.
            '''
        )
    partition_name: str = partition_query_dict.get("partition_name")
    if partition_name and not isinstance(partition_name, str):
        raise ge_exceptions.PartitionerError(
            f'''The type of a partition_name must be a string (Python "str").  The type given is
"{str(type(partition_name))}", which is illegal.
            '''
        )
    partition_definition: dict = partition_query_dict.get("partition_definition")
    if partition_definition:
        if not isinstance(partition_definition, dict):
            raise ge_exceptions.PartitionerError(
                f'''The type of a partition_definition must be a dictionary (Python "dict").  The type given is
"{str(type(partition_definition))}", which is illegal.
                '''
            )
        if not all([isinstance(key, str) for key in partition_definition.keys()]):
            raise ge_exceptions.PartitionerError('All partition_definition keys must strings (Python "str").')
    data_asset_name: str = partition_query_dict.get("data_asset_name")
    if data_asset_name and not isinstance(data_asset_name, str):
        raise ge_exceptions.PartitionerError(
            f'''The type of a data_asset_name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
            '''
        )
    limit: int = partition_query_dict.get("limit")
    if limit and not isinstance(limit, int):
        raise ge_exceptions.PartitionerError(
            f'''The type of a limit must be an integer (Python "int").  The type given is
"{str(type(limit))}", which is illegal.
            '''
        )
    if limit is None or limit < 0:
        limit = sys.maxsize
    return PartitionQuery(
        custom_filter=custom_filter,
        partition_index=partition_index,
        partition_name=partition_name,
        partition_definition=partition_definition,
        data_asset_name=data_asset_name,
        limit=limit
    )


class PartitionQuery(object):
    RECOGNIZED_PARTITION_QUERY_KEYS: set = {
        "custom_filter",
        "partition_index",
        "partition_name",
        "partition_definition",
        "data_asset_name",
        "limit"
    }

    def __init__(
        self,
        custom_filter: Callable = None,
        partition_index: int = None,
        partition_name: str = None,
        partition_definition: dict = None,
        data_asset_name: str = None,
        limit: int = None
    ):
        self._partition_index = partition_index
        self._partition_name = partition_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition
        self._custom_filter = custom_filter
        self._limit = limit

    @property
    def custom_filter(self) -> Callable:
        return self._custom_filter

    @property
    def partition_index(self) -> int:
        return self._partition_index

    @property
    def partition_name(self) -> str:
        return self._partition_name

    @property
    def partition_definition(self) -> dict:
        return self._partition_definition

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def limit(self) -> int:
        return self._limit

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "custom_filter": self.custom_filter,
            "partition_index": self.partition_index,
            "partition_name": self.partition_name,
            "partition_definition": self.partition_definition,
            "data_asset_name": self.data_asset_name,
            "limit": self.limit
        }
        return str(doc_fields_dict)

    def select_partitions(self, partitions: Union[List[Partition], None] = None) -> List[Partition]:
        if partitions is None:
            return []
        if self.custom_filter:
            filter_function: Callable = self.custom_filter
            selected_partitions: List[Partition] = list(
                filter(
                    lambda partition: filter_function(
                        name=partition.name,
                        data_asset_name=partition.data_asset_name,
                        partition_definition=partition.definition
                    ),
                    partitions
                )
            )
            return selected_partitions[:self.limit]
        if self.partition_index:
            return [partitions[self.partition_index]]
        filter_function: Callable = self.best_effort_partition_matcher()
        selected_partitions: List[Partition] = list(
            filter(
                lambda partition: filter_function(
                    partition_name=partition.name,
                    data_asset_name=partition.data_asset_name,
                    partition_definition=partition.definition
                ),
                partitions
            )
        )
        return selected_partitions[:self.limit]

    def best_effort_partition_matcher(self) -> Callable:
        def match_partition_to_query_params(
            partition_name: str,
            data_asset_name: str,
            partition_definition: dict
        ) -> bool:
            if self.partition_name:
                if partition_name != self.partition_name:
                    return False
            if self.partition_definition:
                if not partition_definition:
                    return False
                common_keys: set = set(self.partition_definition.keys()) & set(partition_definition.keys())
                if not common_keys:
                    return False
                for key in common_keys:
                    if partition_definition[key] != self.partition_definition[key]:
                        return False
            if self.data_asset_name:
                if data_asset_name != self.data_asset_name:
                    return False
            return True
        return match_partition_to_query_params
