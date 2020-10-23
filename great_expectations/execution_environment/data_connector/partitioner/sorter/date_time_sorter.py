import datetime
import logging
from typing import Any

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)
from great_expectations.execution_environment.data_connector.partitioner.sorter.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


def parse_string_to_datetime(
    datetime_string: str, datetime_format_string: str
) -> datetime.date:
    if not isinstance(datetime_string, str):
        raise ge_exceptions.SorterError(
            f"""Source "datetime_string" must have string type (actual type is "{str(type(datetime_string))}").
            """
        )
    if datetime_format_string and not isinstance(datetime_format_string, str):
        raise ge_exceptions.SorterError(
            f"""DateTime parsing formatter "datetime_format_string" must have string type (actual type is
"{str(type(datetime_format_string))}").
            """
        )
    return datetime.datetime.strptime(datetime_string, datetime_format_string).date()


def datetime_to_int(dt: datetime.date) -> int:
    return int(dt.strftime("%Y%m%d%H%M%S"))


class DateTimeSorter(Sorter):
    def __init__(self, name: str, orderby: str = "asc", datetime_format="%Y%m%d"):
        super().__init__(name=name, orderby=orderby)

        self._datetime_format = datetime_format

    def get_partition_key(self, partition: Partition) -> Any:
        partition_definition: dict = partition.definition
        partition_value: Any = partition_definition[self.name]
        dt: datetime.date = parse_string_to_datetime(
            datetime_string=partition_value,
            datetime_format_string=self._datetime_format,
        )
        return datetime_to_int(dt=dt)
