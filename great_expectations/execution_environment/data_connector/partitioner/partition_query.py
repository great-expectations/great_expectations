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
            partition_index=None,
            partition_name=None,
            data_asset_name=None,
            partition_definition=None,
            custom_filter=None,
            limit=None
        )
    partition_query_keys: set = set(partition_query_dict.keys())
    if not partition_query_keys <= PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS:
        raise ge_exceptions.PartitionerError(
            f'''Unrecognized partition_query key(s):
"{str(partition_query_keys - PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS)}" detected.
            '''
        )
    partition_index: int = partition_query_dict.get("partition_index")
    if partition_index and not isinstance(partition_index, int):
        raise ge_exceptions.PartitionerError(
            f'''The type of a partition_index must be an integer (Python "int").  The type given is
"{str(type(partition_index))}", which is illegal.
            '''
        )
    partition_name: str = partition_query_dict.get("name")
    if partition_name and not isinstance(partition_name, str):
        raise ge_exceptions.PartitionerError(
            f'''The type of a partition_name must be a string (Python "str").  The type given is
"{str(type(partition_name))}", which is illegal.
            '''
        )
    data_asset_name: str = partition_query_dict.get("data_asset_name")
    if data_asset_name and not isinstance(data_asset_name, str):
        raise ge_exceptions.PartitionerError(
            f'''The type of a data_asset_name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
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
    limit: int = partition_query_dict.get("limit")
    if limit and not isinstance(limit, int):
        raise ge_exceptions.PartitionerError(
            f'''The type of a limit must be an integer (Python "int").  The type given is
"{str(type(limit))}", which is illegal.
            '''
        )
    if limit is None or limit < 0:
        limit = sys.maxsize
    custom_filter: Callable = partition_query_dict.get("custom_filter")
    if custom_filter and not isinstance(custom_filter, Callable):
        raise ge_exceptions.PartitionerError(
            f'''The type of a custom_filter be a function (Python "Callable").  The type given is
"{str(type(custom_filter))}", which is illegal.
            '''
        )
    if custom_filter:
        return PartitionQuery(
            partition_index=None,
            partition_name=None,
            data_asset_name=data_asset_name,
            partition_definition=None,
            custom_filter=custom_filter,
            limit=limit
        )
    if partition_index:
        return PartitionQuery(
            partition_index=partition_index,
            partition_name=None,
            data_asset_name=data_asset_name,
            partition_definition=None,
            custom_filter=None,
            limit=limit
        )
    return PartitionQuery(
        partition_index=None,
        partition_name=partition_name,
        data_asset_name=data_asset_name,
        partition_definition=partition_definition,
        custom_filter=None,
        limit=limit
    )


class PartitionQuery(object):
    RECOGNIZED_PARTITION_QUERY_KEYS: set = {
        "partition_index",
        "partition_name",
        "data_asset_name",
        "partition_definition",
        "custom_filter",
        "limit"
    }

    def __init__(
        self,
        partition_index: int = None,
        partition_name: str = None,
        data_asset_name: str = None,
        partition_definition: dict = None,
        custom_filter: Callable = None,
        limit: int = None
    ):
        self._partition_index = partition_index
        self._partition_name = partition_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition
        self._custom_filter = custom_filter
        self._limit = limit

    @property
    def partition_index(self) -> int:
        return self._partition_index

    @property
    def partition_name(self) -> str:
        return self._partition_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def partition_definition(self) -> dict:
        return self._partition_definition

    @property
    def custom_filter(self) -> Callable:
        return self._custom_filter

    @property
    def limit(self) -> int:
        return self._limit

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "partition_index": self.partition_index,
            "partition_name": self.partition_name,
            "data_asset_name": self.data_asset_name,
            "partition_definition": self.partition_definition,
            "custom_filter": self.custom_filter,
            "limit": self.limit
        }
        return str(doc_fields_dict)

    def select_partitions(self, partitions: Union[List[Partition], None] = None) -> List[Partition]:
        if self.custom_filter:
            filter_function: Callable = self.custom_filter
            return list(
                filter(
                    lambda partition: filter_function(
                        name=partition.name,
                        data_asset_name=partition.data_asset_name,
                        partition_definition=partition.definition
                    ),
                    partitions
                )
            )
        if self.partition_index:
            return [partitions[self.partition_index]]
        filter_function: Callable = self.best_effort_partition_matcher()
        return list(
            filter(
                lambda partition: filter_function(
                    partition_name=partition.name,
                    data_asset_name=partition.data_asset_name,
                    partition_definition=partition.definition
                ),
                partitions
            )
        )

    def best_effort_partition_matcher(self) -> Callable:
        def match_partition_to_query_params(
            partition_name: str,
            data_asset_name: str,
            partition_definition: dict
        ) -> bool:
            if self.partition_definition and partition_definition == self.partition_definition \
                    and self.partition_name and partition_name == self.partition_name \
                    and self.data_asset_name and data_asset_name == self.data_asset_name:
                return True
            if self.partition_name:
                if partition_name != self.partition_name:
                    return False
            if self.data_asset_name:
                if data_asset_name != self.data_asset_name:
                    return False
            return True
        return match_partition_to_query_params
