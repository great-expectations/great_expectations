import datetime
import enum
from typing import Callable, List, Union

from dateutil.parser import parse

from great_expectations.exceptions import exceptions as ge_exceptions

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

try:
    from sqlalchemy.engine import LegacyRow
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.sql import Selectable
    from sqlalchemy.sql.elements import (
        BooleanClauseList,
        Label,
        TextClause,
        quoted_name,
    )
except ImportError:
    reflection = None
    DefaultDialect = None
    Selectable = None
    BooleanClauseList = None
    TextClause = None
    quoted_name = None
    OperationalError = None
    Label = None


class DatePart(enum.Enum):
    """SQL supported date parts for most dialects."""

    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


class SqlAlchemyDataSplitter:
    """Methods for splitting data accessible via SqlAlchemyExecutionEngine."""

    def __init__(self):
        pass

    def get_splitter_method(self, splitter_method_name: str) -> Callable:
        """

        Args:
            splitter_method_name:

        Returns:

        """
        splitter_method_name: str = self.get_splitter_method_name(splitter_method_name)

        return getattr(self, splitter_method_name)

    def get_splitter_method_name(self, splitter_method_name: str) -> str:
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

    def split_on_year(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> BooleanClauseList:
        """Split on year values in column_name.

        Args:
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR],
        )

    def split_on_year_and_month(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> BooleanClauseList:
        """Split on year and month values in column_name.

        Args:
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH],
        )

    def split_on_year_and_month_and_day(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> BooleanClauseList:
        """Split on year and month and day values in column_name.

        Args:
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        )

    def split_on_date_parts(
        self,
        column_name: str,
        batch_identifiers: dict,
        date_parts: Union[List[DatePart], List[str]],
    ) -> BooleanClauseList:
        """Split on date_part values in column_name.

        Values are NOT truncated, for example this will return data for a
        given month (if only month is chosen for date_parts) for ALL years.
        This may be useful for viewing seasonality, but you can also specify
        multiple date_parts to achieve date_trunc like behavior e.g.
        year, month and day.

        Args:
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose date parts
                will be used for splitting or key values of {date_part: date_part_value}
            date_parts: part of the date to be used for splitting e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        if len(date_parts) == 0:
            raise ge_exceptions.InvalidConfigError(
                "date_parts are required when using split_on_date_parts."
            )

        column_batch_identifiers: dict = batch_identifiers[column_name]
        date_parts: List[DatePart] = [
            DatePart(date_part.lower()) if isinstance(date_part, str) else date_part
            for date_part in date_parts
        ]

        if isinstance(column_batch_identifiers, str):
            column_batch_identifiers: datetime.datetime = parse(
                column_batch_identifiers
            )

        if isinstance(column_batch_identifiers, datetime.datetime):
            query: BooleanClauseList = sa.and_(  # noqa: F821
                *[
                    sa.extract(date_part.value, sa.column(column_name))
                    == getattr(column_batch_identifiers, date_part.value)
                    for date_part in date_parts
                ]
            )
        else:
            query: BooleanClauseList = sa.and_(  # noqa: F821
                *[
                    sa.extract(date_part.value, sa.column(column_name))
                    == column_batch_identifiers[date_part.value]
                    for date_part in date_parts
                ]
            )

        return query

    def split_on_whole_table(self, batch_identifiers: dict) -> bool:
        """'Split' by returning the whole table"""

        # return sa.column(column_name) == batch_identifiers[column_name]
        return 1 == 1

    def split_on_column_value(self, column_name: str, batch_identifiers: dict) -> bool:
        """Split using the values in the named column"""

        return sa.column(column_name) == batch_identifiers[column_name]

    def split_on_converted_datetime(
        self,
        column_name: str,
        batch_identifiers: dict,
        date_format_string: str = "%Y-%m-%d",
    ) -> bool:
        """Convert the values in the named column to the given date_format, and split on that"""

        return (
            sa.func.strftime(
                date_format_string,
                sa.column(column_name),
            )
            == batch_identifiers[column_name]
        )

    def split_on_divided_integer(
        self, column_name: str, divisor: int, batch_identifiers: dict
    ) -> bool:
        """Divide the values in the named column by `divisor`, and split on that"""

        return (
            sa.cast(sa.column(column_name) / divisor, sa.Integer)
            == batch_identifiers[column_name]
        )

    def split_on_mod_integer(
        self, column_name: str, mod: int, batch_identifiers: dict
    ) -> bool:
        """Divide the values in the named column by `divisor`, and split on that"""

        return sa.column(column_name) % mod == batch_identifiers[column_name]

    def split_on_multi_column_values(
        self, column_names: List[str], batch_identifiers: dict
    ) -> bool:
        """Split on the joint values in the named columns"""

        return sa.and_(
            *(
                sa.column(column_name) == column_value
                for column_name, column_value in batch_identifiers.items()
            )
        )

    def split_on_hashed_column(
        self,
        column_name: str,
        hash_digits: int,
        batch_identifiers: dict,
    ) -> bool:
        """Split on the hashed value of the named column"""

        return (
            sa.func.right(sa.func.md5(sa.column(column_name)), hash_digits)
            == batch_identifiers[column_name]
        )
