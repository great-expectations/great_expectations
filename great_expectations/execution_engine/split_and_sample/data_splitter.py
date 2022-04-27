import abc
import datetime
import enum
from typing import Callable, List, Union

from dateutil.parser import parse

import great_expectations.exceptions as ge_exceptions


class DatePart(enum.Enum):
    """SQL supported date parts for most dialects."""

    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"

    def __eq__(self, other):
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)


class DataSplitter(abc.ABC):
    """Abstract base class containing methods for splitting data accessible via Execution Engines.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. DataSplitter.date_part.MONTH
    """

    date_part: DatePart = DatePart

    def get_splitter_method(self, splitter_method_name: str) -> Callable:
        """Get the appropriate splitter method from the method name.

        Args:
            splitter_method_name: name of the splitter to retrieve.

        Returns:
            splitter method.
        """
        splitter_method_name: str = self._get_splitter_method_name(splitter_method_name)

        return getattr(self, splitter_method_name)

    def _get_splitter_method_name(self, splitter_method_name: str) -> str:
        """Accept splitter methods with or without starting with `_`.

        Args:
            splitter_method_name: splitter name starting with or without preceding `_`.

        Returns:
            splitter method name stripped of preceding underscore.
        """
        if splitter_method_name.startswith("_"):
            return splitter_method_name[1:]
        else:
            return splitter_method_name

    def _convert_date_parts(
        self, date_parts: Union[List[DatePart], List[str]]
    ) -> List[DatePart]:
        """Convert a list of date parts to DatePart objects.

        Args:
            date_parts: List of DatePart or string representations of DatePart.

        Returns:
            List of DatePart objects
        """
        return [
            DatePart(date_part.lower()) if isinstance(date_part, str) else date_part
            for date_part in date_parts
        ]

    @staticmethod
    def _validate_date_parts(date_parts: Union[List[DatePart], List[str]]) -> None:
        """Validate that date parts exist and are of the correct type.

        Args:
            date_parts: DatePart instances or str.

        Returns:
            None, this method raises exceptions if the config is invalid.
        """
        if len(date_parts) == 0:
            raise ge_exceptions.InvalidConfigError(
                "date_parts are required when using split_on_date_parts."
            )
        if not all(
            [(isinstance(dp, DatePart)) or (isinstance(dp, str)) for dp in date_parts]
        ):
            raise ge_exceptions.InvalidConfigError(
                "date_parts should be of type DatePart or str."
            )

    @staticmethod
    def _verify_all_strings_are_valid_date_parts(date_part_strings: List[str]) -> None:
        [DatePart(date_part_string) for date_part_string in date_part_strings]

    def _convert_datetime_batch_identifiers_to_date_parts_dict(
        self,
        column_batch_identifiers: Union[datetime.datetime, str, dict],
        date_parts: List[DatePart],
    ) -> dict:
        """Convert batch identifiers to a dict of {date_part as str: date_part value}.

        Args:
            column_batch_identifiers: Batch identifiers related to the column of interest.
            date_parts: List of DatePart to include in the return value.

        Returns:
            A dict of {date_part as str: date_part value} eg. {"day": 3}.
        """

        if isinstance(column_batch_identifiers, str):
            column_batch_identifiers: datetime.datetime = parse(
                column_batch_identifiers
            )

        if isinstance(column_batch_identifiers, datetime.datetime):
            return {
                date_part.value: getattr(column_batch_identifiers, date_part.value)
                for date_part in date_parts
            }
        else:
            self._verify_all_strings_are_valid_date_parts(
                list(column_batch_identifiers.keys())
            )
            return column_batch_identifiers
