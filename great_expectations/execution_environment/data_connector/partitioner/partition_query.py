import itertools
import logging
import sys
from typing import Callable, Dict, List, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.util import is_int

logger = logging.getLogger(__name__)


def build_partition_query(
    partition_query_dict: Union[
        Dict[str, Union[int, list, tuple, slice, str, Dict, Callable, None]], None
    ] = None
):
    if not partition_query_dict:
        return PartitionQuery(
            custom_filter=None,
            partition_name=None,
            partition_definition=None,
            data_asset_name=None,
            limit=None,
            partition_index=None,
        )
    partition_query_keys: set = set(partition_query_dict.keys())
    if not partition_query_keys <= PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS:
        raise ge_exceptions.PartitionerError(
            f"""Unrecognized partition_query key(s):
"{str(partition_query_keys - PartitionQuery.RECOGNIZED_PARTITION_QUERY_KEYS)}" detected.
            """
        )
    custom_filter: Callable = partition_query_dict.get("custom_filter")
    if custom_filter and not isinstance(custom_filter, Callable):
        raise ge_exceptions.PartitionerError(
            f"""The type of a custom_filter be a function (Python "Callable").  The type given is
"{str(type(custom_filter))}", which is illegal.
            """
        )
    partition_name: str = partition_query_dict.get("partition_name")
    if partition_name and not isinstance(partition_name, str):
        raise ge_exceptions.PartitionerError(
            f"""The type of a partition_name must be a string (Python "str").  The type given is
"{str(type(partition_name))}", which is illegal.
            """
        )
    partition_definition: dict = partition_query_dict.get("partition_definition")
    if partition_definition:
        if not isinstance(partition_definition, dict):
            raise ge_exceptions.PartitionerError(
                f"""The type of a partition_definition must be a dictionary (Python "dict").  The type given is
"{str(type(partition_definition))}", which is illegal.
                """
            )
        if not all([isinstance(key, str) for key in partition_definition.keys()]):
            raise ge_exceptions.PartitionerError(
                'All partition_definition keys must strings (Python "str").'
            )
    data_asset_name: str = partition_query_dict.get("data_asset_name")
    if data_asset_name and not isinstance(data_asset_name, str):
        raise ge_exceptions.PartitionerError(
            f"""The type of a data_asset_name must be a string (Python "str").  The type given is
"{str(type(data_asset_name))}", which is illegal.
            """
        )
    limit: int = partition_query_dict.get("limit")
    if limit and not isinstance(limit, int):
        raise ge_exceptions.PartitionerError(
            f"""The type of a limit must be an integer (Python "int").  The type given is
"{str(type(limit))}", which is illegal.
            """
        )
    if limit is None or limit < 0:
        limit = sys.maxsize
    partition_index: Union[int, list, tuple, slice, str] = partition_query_dict.get(
        "partition_index"
    )
    partition_index = _parse_partition_index(partition_index=partition_index)
    return PartitionQuery(
        custom_filter=custom_filter,
        partition_name=partition_name,
        partition_definition=partition_definition,
        data_asset_name=data_asset_name,
        limit=limit,
        partition_index=partition_index,
    )


def _parse_partition_index(
    partition_index: Union[int, list, tuple, slice, str, None] = None
) -> Union[int, slice, None]:
    if partition_index is None:
        return None
    elif isinstance(partition_index, (int, slice)):
        return partition_index
    elif isinstance(partition_index, (list, tuple)):
        if len(partition_index) > 3:
            raise ge_exceptions.PartitionerError(
                f"""The number of partition_index slice components must be between 1 and 3 (the given number is
{len(partition_index)}).
                """
            )
        if len(partition_index) == 1:
            return _parse_partition_index(partition_index=partition_index[0])
        if len(partition_index) == 2:
            return slice(partition_index[0], partition_index[1], None)
        if len(partition_index) == 3:
            return slice(partition_index[0], partition_index[1], partition_index[2])
    elif isinstance(partition_index, str):
        if is_int(value=partition_index):
            return _parse_partition_index(partition_index=int(partition_index))
        return _parse_partition_index(
            partition_index=[int(idx_str) for idx_str in partition_index.split(":")]
        )
    else:
        raise ge_exceptions.PartitionerError(
            f"""The type of a partition_index must be an integer (Python "int"), or a list (Python "list") or a tuple
(Python "tuple"), or a Python "slice" object, or a string that has the format of a single integer or a slice argument.
The type given is "{str(type(partition_index))}", which is illegal.
            """
        )


class PartitionQuery:
    RECOGNIZED_PARTITION_QUERY_KEYS: set = {
        "custom_filter",
        "partition_name",
        "partition_definition",
        "data_asset_name",
        "limit",
        "partition_index",
    }

    def __init__(
        self,
        custom_filter: Callable = None,
        partition_name: str = None,
        partition_definition: dict = None,
        data_asset_name: str = None,
        limit: int = None,
        partition_index: Union[slice, None] = None,
    ):
        self._custom_filter = custom_filter
        self._partition_name = partition_name
        self._data_asset_name = data_asset_name
        self._partition_definition = partition_definition
        self._limit = limit
        self._partition_index = partition_index

    @property
    def custom_filter(self) -> Callable:
        return self._custom_filter

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

    @property
    def partition_index(self) -> Union[int, list, tuple, slice, str, None]:
        return self._partition_index

    def __repr__(self) -> str:
        doc_fields_dict: dict = {
            "custom_filter": self.custom_filter,
            "partition_name": self.partition_name,
            "partition_definition": self.partition_definition,
            "data_asset_name": self.data_asset_name,
            "limit": self.limit,
            "partition_index": self.partition_index,
        }
        return str(doc_fields_dict)

    def select_partitions(
        self, partitions: Union[List[Partition], None] = None
    ) -> List[Partition]:
        if partitions is None:
            return []
        filter_function: Callable
        if self.custom_filter:
            filter_function = self.custom_filter
        else:
            filter_function = self.best_effort_partition_matcher()
        selected_partitions: List[Partition] = list(
            filter(
                lambda partition: filter_function(
                    data_asset_name=partition.data_asset_name,
                    partition_name=partition.name,
                    partition_definition=partition.definition,
                ),
                partitions,
            )
        )
        selected_partitions = selected_partitions[: self.limit]
        if self.partition_index is not None:
            if isinstance(self.partition_index, int):
                selected_partitions = [selected_partitions[self.partition_index]]
            else:
                selected_partitions = list(
                    itertools.chain.from_iterable(
                        [selected_partitions[self.partition_index]]
                    )
                )
        return selected_partitions

    def best_effort_partition_matcher(self) -> Callable:
        def match_partition_to_query_params(
            data_asset_name: str, partition_name: str, partition_definition: dict
        ) -> bool:
            if self.partition_name:
                if partition_name != self.partition_name:
                    return False
            if self.partition_definition:
                if not partition_definition:
                    return False
                partition_definition_query_keys: set = set(
                    self.partition_definition.keys()
                )
                actual_partition_definition_keys: set = set(partition_definition.keys())
                if (
                    not partition_definition_query_keys
                    <= actual_partition_definition_keys
                ):
                    raise ge_exceptions.PartitionerError(
                        f"""Unrecognized partition_definition query key(s):
"{str(partition_definition_query_keys - actual_partition_definition_keys)}" detected.
                        """
                    )
                if not partition_definition_query_keys:
                    return False
                for key in partition_definition_query_keys:
                    if partition_definition[key] != self.partition_definition[key]:
                        return False
            if self.data_asset_name:
                if data_asset_name != self.data_asset_name:
                    return False
            return True

        return match_partition_to_query_params
