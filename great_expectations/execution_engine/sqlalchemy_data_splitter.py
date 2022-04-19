import datetime
import enum
from typing import List, Union

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
