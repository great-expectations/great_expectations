import datetime
import logging
from typing import Any

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchDefinition  # noqa: TCH001
from great_expectations.core.util import datetime_to_int, parse_string_to_datetime
from great_expectations.experimental.datasources.data_asset.data_connector.sorter import (
    Sorter,
)

logger = logging.getLogger(__name__)


class DateTimeSorter(Sorter):
    def __init__(
        self, name: str, orderby: str = "asc", datetime_format="%Y%m%d"
    ) -> None:
        super().__init__(name=name, orderby=orderby)

        if datetime_format and not isinstance(datetime_format, str):
            raise gx_exceptions.SorterError(
                f"""DateTime parsing formatter "datetime_format_string" must have string type (actual type is
"{str(type(datetime_format))}").
"""
            )

        # TODO: <Alex>ALEX-USE_PYDANTIC_VALIDATOR</Alex>
        self._datetime_format = datetime_format
        # TODO: <Alex>ALEX</Alex>

    def get_batch_key(self, batch_definition: BatchDefinition) -> Any:
        batch_identifiers: dict = batch_definition.batch_identifiers
        partition_value: Any = batch_identifiers[self.key]
        dt: datetime.date = parse_string_to_datetime(
            datetime_string=partition_value,
            datetime_format_string=self._datetime_format,
        )
        return datetime_to_int(dt=dt)
