"""Create queries for use in sql data splitting.

This module contains utilities for generating queries used by execution engines
and data connectors to split data into batches based on the data itself. It
is typically used from within either an execution engine or a data connector,
not by itself.

    Typical usage example:
        __init__():
            self._sqlalchemy_data_splitter = SqlAlchemyDataSplitter()

        elsewhere():
            splitter = self._sqlalchemy_data_splitter._get_splitter_method()
            split_query_or_clause = splitter()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.split_and_sample.data_splitter import (
    DataSplitter,
    DatePart,
    SplitterMethod,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

try:
    import sqlalchemy.sql.functions.concat as concat
    from sqlalchemy.engine import LegacyRow
    from sqlalchemy.sql import Selectable
    from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList, Label
except ImportError:
    LegacyRow = None
    Selectable = None
    BinaryExpression = None
    BooleanClauseList = None
    Label = None
    concat = None

if TYPE_CHECKING:
    from great_expectations.execution_engine.sqlalchemy_execution_engine import (
        SqlAlchemyExecutionEngine,
    )


class SqlAlchemyDataSplitter(DataSplitter):
    """Methods for splitting data accessible via SqlAlchemyExecutionEngine.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. SqlAlchemyDataSplitter.date_part.MONTH
    """

    def __init__(self, dialect: str):
        self._dialect = dialect

    DATETIME_SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        SplitterMethod.SPLIT_ON_YEAR: "get_data_for_batch_identifiers_year",
        SplitterMethod.SPLIT_ON_YEAR_AND_MONTH: "get_data_for_batch_identifiers_year_and_month",
        SplitterMethod.SPLIT_ON_YEAR_AND_MONTH_AND_DAY: "get_data_for_batch_identifiers_year_and_month_and_day",
        SplitterMethod.SPLIT_ON_DATE_PARTS: "get_data_for_batch_identifiers_for_split_on_date_parts",
    }

    SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        SplitterMethod.SPLIT_ON_WHOLE_TABLE: "get_split_query_for_data_for_batch_identifiers_for_split_on_whole_table",
        SplitterMethod.SPLIT_ON_COLUMN_VALUE: "get_split_query_for_data_for_batch_identifiers_for_split_on_column_value",
        SplitterMethod.SPLIT_ON_CONVERTED_DATETIME: "get_split_query_for_data_for_batch_identifiers_for_split_on_converted_datetime",
        SplitterMethod.SPLIT_ON_DIVIDED_INTEGER: "get_split_query_for_data_for_batch_identifiers_for_split_on_divided_integer",
        SplitterMethod.SPLIT_ON_MOD_INTEGER: "get_split_query_for_data_for_batch_identifiers_for_split_on_mod_integer",
        SplitterMethod.SPLIT_ON_MULTI_COLUMN_VALUES: "get_split_query_for_data_for_batch_identifiers_for_split_on_multi_column_values",
        SplitterMethod.SPLIT_ON_HASHED_COLUMN: "get_split_query_for_data_for_batch_identifiers_for_split_on_hashed_column",
    }

    def split_on_year(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> Union[BinaryExpression, BooleanClauseList]:
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
    ) -> Union[BinaryExpression, BooleanClauseList]:
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
    ) -> Union[BinaryExpression, BooleanClauseList]:
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
    ) -> Union[BinaryExpression, BooleanClauseList]:
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
        self._validate_date_parts(date_parts)

        date_parts: List[DatePart] = self._convert_date_parts(date_parts)

        column_batch_identifiers: dict = batch_identifiers[column_name]

        date_parts_dict: dict = (
            self._convert_datetime_batch_identifiers_to_date_parts_dict(
                column_batch_identifiers, date_parts
            )
        )

        query: Union[BinaryExpression, BooleanClauseList] = sa.and_(
            *[
                sa.extract(date_part.value, sa.column(column_name))
                == date_parts_dict[date_part.value]
                for date_part in date_parts
            ]
        )

        return query

    @staticmethod
    def split_on_whole_table(batch_identifiers: dict) -> bool:
        """'Split' by returning the whole table"""

        return True

    @staticmethod
    def split_on_column_value(column_name: str, batch_identifiers: dict) -> bool:
        """Split using the values in the named column"""

        return sa.column(column_name) == batch_identifiers[column_name]

    def split_on_converted_datetime(
        self,
        column_name: str,
        batch_identifiers: dict,
        date_format_string: str = "%Y-%m-%d",
    ) -> bool:
        """Convert the values in the named column to the given date_format, and split on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.func.strftime(
                    date_format_string,
                    sa.column(column_name),
                )
                == batch_identifiers[column_name]
            )

        raise NotImplementedError(
            f'Splitter method "split_on_converted_datetime" is not supported for "{self._dialect}" SQL dialect.'
        )

    def split_on_divided_integer(
        self,
        column_name: str,
        divisor: int,
        batch_identifiers: dict,
    ) -> bool:
        """Divide the values in the named column by `divisor`, and split on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.cast(
                    (sa.cast(sa.column(column_name), sa.Integer) / divisor), sa.Integer
                )
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.MYSQL:
            return (
                sa.cast(
                    sa.func.truncate(
                        (sa.cast(sa.column(column_name), sa.Integer) / divisor), 0
                    ),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.MSSQL:
            return (
                sa.cast(
                    sa.func.round(
                        (sa.cast(sa.column(column_name), sa.Integer) / divisor), 0, 1
                    ),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.AWSATHENA:
            return (
                sa.cast(
                    sa.func.truncate(
                        sa.cast(sa.column(column_name), sa.Integer) / divisor
                    ),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        return (
            sa.cast(
                sa.func.trunc(
                    (sa.cast(sa.column(column_name), sa.Integer) / divisor), 0
                ),
                sa.Integer,
            )
            == batch_identifiers[column_name]
        )

    def split_on_mod_integer(
        self,
        column_name: str,
        mod: int,
        batch_identifiers: dict,
    ) -> bool:
        """Divide the values in the named column by `mod`, and split on that"""
        if self._dialect in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.MSSQL,
        ]:
            return (
                sa.cast(sa.column(column_name), sa.Integer) % mod
                == batch_identifiers[column_name]
            )

        return (
            sa.func.mod(sa.cast(sa.column(column_name), sa.Integer), mod)
            == batch_identifiers[column_name]
        )

    @staticmethod
    def split_on_multi_column_values(
        column_names: List[str],
        batch_identifiers: dict,
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
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.func.md5(sa.cast(sa.column(column_name), sa.VARCHAR), hash_digits)
                == batch_identifiers[column_name]
            )

        raise NotImplementedError(
            f'Splitter method "split_on_hashed_column" is not supported for "{self._dialect}" SQL dialect.'
        )

    def get_data_for_batch_identifiers(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        splitter_method_name: str,
        splitter_kwargs: dict,
    ) -> List[dict]:
        """Build data used to construct batch identifiers for the input table using the provided splitter config.

        Sql splitter configurations yield the unique values that comprise a batch by introspecting your data.

        Args:
            execution_engine: Used to introspect the data.
            table_name: Table to split.
            splitter_method_name: Desired splitter method to use.
            splitter_kwargs: Dict of directives used by the splitter method as keyword arguments of key=value.

        Returns:
            List of dicts of the form [{column_name: {"key": value}}]
        """
        processed_splitter_method_name: str = self._get_splitter_method_name(
            splitter_method_name
        )
        if self._is_datetime_splitter(processed_splitter_method_name):
            splitter_fn_name: str = self.DATETIME_SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING[
                processed_splitter_method_name
            ]
            batch_identifiers_list: List[dict] = getattr(self, splitter_fn_name)(
                execution_engine, table_name, **splitter_kwargs
            )
        else:
            batch_identifiers_list: List[
                dict
            ] = self.get_data_for_batch_identifiers_for_non_date_part_splitters(
                execution_engine,
                table_name,
                processed_splitter_method_name,
                splitter_kwargs,
            )

        return batch_identifiers_list

    def _is_datetime_splitter(self, splitter_method_name: str) -> bool:
        """Whether the splitter method is a datetime splitter.

        Args:
            splitter_method_name: Name of the splitter method

        Returns:
            Boolean
        """
        return splitter_method_name in list(
            self.DATETIME_SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING.keys()
        )

    def get_data_for_batch_identifiers_year(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            table_name: table to split.
            column_name: column in table to use in determining split.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022}}]
        """
        return self.get_data_for_batch_identifiers_for_split_on_date_parts(
            execution_engine=execution_engine,
            table_name=table_name,
            column_name=column_name,
            date_parts=[DatePart.YEAR],
        )

    def get_data_for_batch_identifiers_year_and_month(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year and month.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            table_name: table to split.
            column_name: column in table to use in determining split.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022, "month": 4}}]
        """
        return self.get_data_for_batch_identifiers_for_split_on_date_parts(
            execution_engine=execution_engine,
            table_name=table_name,
            column_name=column_name,
            date_parts=[DatePart.YEAR, DatePart.MONTH],
        )

    def get_data_for_batch_identifiers_year_and_month_and_day(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year and month and day.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            table_name: table to split.
            column_name: column in table to use in determining split.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022, "month": 4, "day": 14}}]
        """
        return self.get_data_for_batch_identifiers_for_split_on_date_parts(
            execution_engine=execution_engine,
            table_name=table_name,
            column_name=column_name,
            date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        )

    def get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts(
        self,
        table_name: str,
        column_name: str,
        date_parts: Union[List[DatePart], List[str]],
    ) -> Selectable:
        """Build a split query to retrieve batch_identifiers info from a column split on a list of date parts.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            table_name: table to split.
            column_name: column in table to use in determining split.
            date_parts: part of the date to be used for splitting e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """
        self._validate_date_parts(date_parts)

        date_parts: List[DatePart] = self._convert_date_parts(date_parts)

        # NOTE: AJB 20220414 concatenating to find distinct values to support all dialects.
        # There are more performant dialect-specific methods that can be implemented in
        # future improvements.
        # NOTE: AJB 20220511 for awsathena we need to cast extracted date parts
        # to string first before concatenating them.
        if len(date_parts) == 1:
            # MSSql does not accept single item concatenation
            concat_clause: List[Label] = [
                sa.func.distinct(
                    sa.func.extract(date_parts[0].value, sa.column(column_name)).label(
                        date_parts[0].value
                    )
                ).label("concat_distinct_values")
            ]
        else:
            """
            # NOTE: <Alex>6/29/2022</Alex>
            Certain SQLAlchemy-compliant backends (e.g., Amazon Redshift, SQLite) allow only binary operators for "CONCAT".
            """
            if self._dialect == GXSqlDialect.SQLITE:
                concat_date_parts = sa.cast(
                    sa.func.extract(date_parts[0].value, sa.column(column_name)),
                    sa.String,
                )

                date_part: DatePart
                for date_part in date_parts[1:]:
                    concat_date_parts = concat_date_parts.concat(
                        sa.cast(
                            sa.func.extract(date_part.value, sa.column(column_name)),
                            sa.String,
                        )
                    )

                concat_clause: List[Label] = [
                    sa.func.distinct(concat_date_parts).label("concat_distinct_values"),
                ]
            else:
                concat_date_parts: concat = sa.func.concat(
                    "",
                    sa.cast(
                        sa.func.extract(date_parts[0].value, sa.column(column_name)),
                        sa.String,
                    ),
                )

                date_part: DatePart
                for date_part in date_parts[1:]:
                    concat_date_parts = sa.func.concat(
                        concat_date_parts,
                        sa.cast(
                            sa.func.extract(date_part.value, sa.column(column_name)),
                            sa.String,
                        ),
                    )

                concat_clause: List[Label] = [
                    sa.func.distinct(concat_date_parts).label("concat_distinct_values"),
                ]

        split_query: Selectable = sa.select(
            concat_clause
            + [
                sa.cast(
                    sa.func.extract(date_part.value, sa.column(column_name)), sa.Integer
                ).label(date_part.value)
                for date_part in date_parts
            ]
        ).select_from(sa.text(table_name))

        return split_query

    def get_data_for_batch_identifiers_for_split_on_date_parts(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        column_name: str,
        date_parts: Union[List[DatePart], List[str]],
    ) -> List[dict]:
        """Build batch_identifiers from a column split on a list of date parts.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: used to query the data to find batch identifiers.
            table_name: table to split.
            column_name: column in table to use in determining split.
            date_parts: part of the date to be used for splitting e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """

        split_query: Selectable = (
            self.get_split_query_for_data_for_batch_identifiers_for_split_on_date_parts(
                table_name, column_name, date_parts
            )
        )

        result: List[LegacyRow] = self._execute_split_query(
            execution_engine, split_query
        )

        return self._get_params_for_batch_identifiers_from_date_part_splitter(
            column_name, result, date_parts
        )

    @staticmethod
    def _execute_split_query(
        execution_engine: SqlAlchemyExecutionEngine,
        split_query: Selectable,
    ) -> List[LegacyRow]:
        """Use the provided execution engine to run the split query and fetch all of the results.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            split_query: Query to be executed as a sqlalchemy Selectable.

        Returns:
            List of row results.
        """
        return execution_engine.execute_split_query(split_query)

    def _get_params_for_batch_identifiers_from_date_part_splitter(
        self, column_name: str, result: List[LegacyRow], date_parts: List[DatePart]
    ) -> List[dict]:
        """Get parameters used to build BatchIdentifiers from the results of a get_data_for_batch_identifiers_for_split_on_date_parts

        Args:
            column_name: Column name associated with the batch identifier.
            result: list of LegacyRow objects from sqlalchemy query result.
            date_parts: part of the date to be used for constructing the batch identifiers e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """
        date_parts: List[DatePart] = self._convert_date_parts(date_parts)

        data_for_batch_identifiers: List[dict] = [
            {
                column_name: {
                    date_part.value: getattr(row, date_part.value)
                    for date_part in date_parts
                }
            }
            for row in result
        ]
        return data_for_batch_identifiers

    @staticmethod
    def _get_column_names_from_splitter_kwargs(splitter_kwargs) -> List[str]:
        column_names: List[str] = []

        if "column_names" in splitter_kwargs:
            column_names = splitter_kwargs["column_names"]
        elif "column_name" in splitter_kwargs:
            column_names = [splitter_kwargs["column_name"]]

        return column_names

    def get_data_for_batch_identifiers_for_non_date_part_splitters(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        table_name: str,
        splitter_method_name: str,
        splitter_kwargs: dict,
    ) -> List[dict]:
        """Build data used to construct batch identifiers for the input table using the provided splitter config.

        Sql splitter configurations yield the unique values that comprise a batch by introspecting your data.

        Args:
            execution_engine: Used to introspect the data.
            table_name: Table to split.
            splitter_method_name: Desired splitter method to use.
            splitter_kwargs: Dict of directives used by the splitter method as keyword arguments of key=value.

        Returns:
            List of dicts of the form [{column_name: {"key": value}}]
        """
        get_split_query_method_name: str = (
            self._get_method_name_for_get_data_for_batch_identifiers_method(
                splitter_method_name
            )
        )

        split_query: Selectable = getattr(self, get_split_query_method_name)(
            table_name=table_name, **splitter_kwargs
        )
        rows: List[LegacyRow] = self._execute_split_query(execution_engine, split_query)
        column_names: List[str] = self._get_column_names_from_splitter_kwargs(
            splitter_kwargs
        )
        return self._get_params_for_batch_identifiers_from_non_date_part_splitters(
            column_names, rows
        )

    def _get_method_name_for_get_data_for_batch_identifiers_method(
        self, splitter_method_name: str
    ):
        """Get the matching method name to get the data for batch identifiers from the input splitter method name.

        Args:
            splitter_method_name: Configured splitter name.

        Returns:
            Name of the corresponding method to get data for building batch identifiers.
        """
        processed_splitter_method_name: str = self._get_splitter_method_name(
            splitter_method_name
        )
        try:
            return self.SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING[
                processed_splitter_method_name
            ]
        except ValueError:
            raise ge_exceptions.InvalidConfigError(
                f"Please provide a supported splitter method name, you provided: {splitter_method_name}"
            )

    @staticmethod
    def _get_params_for_batch_identifiers_from_non_date_part_splitters(
        column_names: List[str],
        rows: List[LegacyRow],
    ) -> List[dict]:
        """Get params used in batch identifiers from output of executing query for non date part splitters.

        Args:
            column_names: Column names referenced in splitter_kwargs.
            rows: Rows from execution of split query.

        Returns:
            Dict of {column_name: row, column_name: row, ...}
        """
        return [dict(zip(column_names, row)) for row in rows]

    @staticmethod
    def get_split_query_for_data_for_batch_identifiers_for_split_on_whole_table(
        table_name: str,
    ) -> Selectable:
        """
        'Split' by returning the whole table

        Note: the table_name parameter is a required to keep the signature of this method consistent with other methods.
        """
        return sa.select([sa.true()])

    @staticmethod
    def get_split_query_for_data_for_batch_identifiers_for_split_on_column_value(
        table_name: str,
        column_name: str,
    ) -> Selectable:
        """Split using the values in the named column"""
        return (
            sa.select([sa.func.distinct(sa.column(column_name))])
            .select_from(sa.text(table_name))
            .order_by(sa.column(column_name).asc())
        )

    def get_split_query_for_data_for_batch_identifiers_for_split_on_converted_datetime(
        self,
        table_name: str,
        column_name: str,
        date_format_string: str = "%Y-%m-%d",
    ) -> Selectable:
        """Convert the values in the named column to the given date_format, and split on that"""
        if self._dialect == "sqlite":
            return sa.select(
                [
                    sa.func.distinct(
                        sa.func.strftime(
                            date_format_string,
                            sa.column(column_name),
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        raise NotImplementedError(
            f'Splitter method "split_on_converted_datetime" is not supported for "{self._dialect}" SQL dialect.'
        )

    def get_split_query_for_data_for_batch_identifiers_for_split_on_divided_integer(
        self,
        table_name: str,
        column_name: str,
        divisor: int,
    ) -> Selectable:
        """Divide the values in the named column by `divisor`, and split on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return sa.select(
                [
                    sa.func.distinct(
                        sa.cast(
                            (sa.cast(sa.column(column_name), sa.Integer) / divisor),
                            sa.Integer,
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        if self._dialect == GXSqlDialect.MYSQL:
            return sa.select(
                [
                    sa.func.distinct(
                        sa.cast(
                            sa.func.truncate(
                                (sa.cast(sa.column(column_name), sa.Integer) / divisor),
                                0,
                            ),
                            sa.Integer,
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        if self._dialect == GXSqlDialect.MSSQL:
            return sa.select(
                [
                    sa.func.distinct(
                        sa.cast(
                            sa.func.round(
                                (sa.cast(sa.column(column_name), sa.Integer) / divisor),
                                0,
                                1,
                            ),
                            sa.Integer,
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        if self._dialect == GXSqlDialect.AWSATHENA:
            return sa.select(
                [
                    sa.func.distinct(
                        sa.cast(
                            sa.func.truncate(
                                sa.cast(sa.column(column_name), sa.Integer) / divisor
                            ),
                            sa.Integer,
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        return sa.select(
            [
                sa.func.distinct(
                    sa.cast(
                        sa.func.trunc(
                            (sa.cast(sa.column(column_name), sa.Integer) / divisor), 0
                        ),
                        sa.Integer,
                    )
                )
            ]
        ).select_from(sa.text(table_name))

    def get_split_query_for_data_for_batch_identifiers_for_split_on_mod_integer(
        self,
        table_name: str,
        column_name: str,
        mod: int,
    ) -> Selectable:
        """Divide the values in the named column by `mod`, and split on that"""
        if self._dialect in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.MSSQL,
        ]:
            return sa.select(
                [sa.func.distinct(sa.cast(sa.column(column_name), sa.Integer) % mod)]
            ).select_from(sa.text(table_name))

        return sa.select(
            [
                sa.func.distinct(
                    sa.func.mod(sa.cast(sa.column(column_name), sa.Integer), mod)
                )
            ]
        ).select_from(sa.text(table_name))

    @staticmethod
    def get_split_query_for_data_for_batch_identifiers_for_split_on_multi_column_values(
        table_name: str,
        column_names: List[str],
    ) -> Selectable:
        """Split on the joint values in the named columns"""
        return (
            sa.select([sa.column(column_name) for column_name in column_names])
            .distinct()
            .select_from(sa.text(table_name))
        )

    def get_split_query_for_data_for_batch_identifiers_for_split_on_hashed_column(
        self,
        table_name: str,
        column_name: str,
        hash_digits: int,
    ) -> Selectable:
        """Note: this method is experimental. It does not work with all SQL dialects."""
        if self._dialect == GXSqlDialect.SQLITE:
            return sa.select(
                [
                    sa.func.distinct(
                        sa.func.md5(
                            sa.cast(sa.column(column_name), sa.VARCHAR), hash_digits
                        )
                    )
                ]
            ).select_from(sa.text(table_name))

        raise NotImplementedError(
            f'Splitter method "split_on_hashed_column" is not supported for "{self._dialect}" SQL dialect.'
        )
