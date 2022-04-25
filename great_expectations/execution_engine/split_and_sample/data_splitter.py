import abc
import enum
from typing import Callable, List, Union


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
