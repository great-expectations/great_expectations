# -*- coding: utf-8 -*-

from typing import Any
import datetime
import logging

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter

logger = logging.getLogger(__name__)


def parse_string_to_datetime(datetime_string: str, datetime_format_string: str = "%Y%m%d") -> datetime.date:
    if not isinstance(datetime_string, str):
        raise ValueError(
            'Source "datetime_string" must have string type (actual type is "{}").'.format(
                str(type(datetime_string))
            )
        )
    if datetime_format_string and not isinstance(datetime_format_string, str):
        raise ValueError(
            'DateTime parsing formatter "datetime_format_string" must have string type (actual type is "{}").'.format(
                str(type(datetime_format_string))
            )
        )
    return datetime.datetime.strptime(datetime_string, datetime_format_string).date()


def datetime_to_int(dt: datetime.date) -> int:
    return int(dt.strftime("%Y%m%d%H%M%S"))


class DateTimeSorter(Sorter):
    r"""
    DateTimeSorter help
    """
    def __init__(self, name: str, **kwargs):
        datetime_format: str = kwargs.get("datetime_format")
        self._datetime_format = datetime_format
        print(f'[ALEX_DEV:DATETIME_SORTER#get_partition_key] DATIMEFORMAT: {self._datetime_format}')
        super().__init__(name=name, **kwargs)

    def get_partition_key(self, partition: Partition) -> Any:
        print(f'[ALEX_DEV:DATETIME_SORTER#get_partition_key] NAME: {self._name} ; ORDERBY: {self._orderby}')
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        print(f'[ALEX_DEV:DATETIME_SORTER#get_partition_key] PARTITION_DEFINITION: {partition_definition} ; PARTITION_VALUE: {partition_value}')
        dt: datetime.date = parse_string_to_datetime(
            datetime_string=partition_value, datetime_format_string=self.datetime_format
        )
        return datetime_to_int(dt=dt)

    @property
    def datetime_format(self) -> str:
        return self._datetime_format
