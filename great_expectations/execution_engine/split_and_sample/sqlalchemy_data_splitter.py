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

from typing import List, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine.split_and_sample.data_splitter import (
    DataSplitter,
    DatePart,
)

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

try:
    from sqlalchemy.engine import LegacyRow
    from sqlalchemy.sql import Selectable
    from sqlalchemy.sql.elements import BinaryExpression, BooleanClauseList, Label
except ImportError:
    LegacyRow = None
    Selectable = None
    BinaryExpression = None
    BooleanClauseList = None
    Label = None


class SqlAlchemyDataSplitter(DataSplitter):
    """Methods for splitting data accessible via SqlAlchemyExecutionEngine.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. SqlAlchemyDataSplitter.date_part.MONTH
    """

    DATETIME_SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        "split_on_year": "get_data_for_batch_identifiers_year",
        "split_on_year_and_month": "get_data_for_batch_identifiers_year_and_month",
        "split_on_year_and_month_and_day": "get_data_for_batch_identifiers_year_and_month_and_day",
        "split_on_date_parts": "get_data_for_batch_identifiers_for_split_on_date_parts",
    }

    SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        "split_on_whole_table": "get_split_query_for_data_for_batch_identifiers_for_split_on_whole_table",
        "split_on_column_value": "get_split_query_for_data_for_batch_identifiers_for_split_on_column_value",
        "split_on_converted_datetime": "get_split_query_for_data_for_batch_identifiers_for_split_on_converted_datetime",
        "split_on_divided_integer": "get_split_query_for_data_for_batch_identifiers_for_split_on_divided_integer",
        "split_on_mod_integer": "get_split_query_for_data_for_batch_identifiers_for_split_on_mod_integer",
        "split_on_multi_column_values": "get_split_query_for_data_for_batch_identifiers_for_split_on_multi_column_values",
        "split_on_hashed_column": "get_split_query_for_data_for_batch_identifiers_for_split_on_hashed_column",
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

    def split_on_whole_table(self, batch_identifiers: dict) -> bool:
        """'Split' by returning the whole table"""

        return True

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

    def get_data_for_batch_identifiers(
        self,
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
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
        if self._is_datetime_splitter(splitter_method_name):
            splitter_fn_name: str = self.DATETIME_SPLITTER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING[
                splitter_method_name
            ]
            batch_identifiers_list: List[dict] = getattr(self, splitter_fn_name)(
                execution_engine, table_name, **splitter_kwargs
            )
        else:
            batch_identifiers_list: List[
                dict
            ] = self.get_data_for_batch_identifiers_for_non_date_part_splitters(
                execution_engine, table_name, splitter_method_name, splitter_kwargs
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
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
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
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year and month.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
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
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
        table_name: str,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column split on year and month and day.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
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
            concat_clause: List[Label] = [
                sa.func.distinct(
                    sa.func.concat(
                        *[
                            (
                                sa.cast(
                                    sa.func.extract(
                                        date_part.value, sa.column(column_name)
                                    ),
                                    sa.String,
                                )
                            ).label(date_part.value)
                            for date_part in date_parts
                        ]
                    )
                ).label("concat_distinct_values"),
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
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
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

    def _execute_split_query(
        self,
        execution_engine: "SqlAlchemyExecutionEngine",
        split_query: Selectable,  # noqa: F821
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

    def _get_column_names_from_splitter_kwargs(self, splitter_kwargs) -> List[str]:
        column_names: List[str] = []

        if "column_names" in splitter_kwargs:
            column_names = splitter_kwargs["column_names"]
        elif "column_name" in splitter_kwargs:
            column_names = [splitter_kwargs["column_name"]]

        return column_names

    def get_data_for_batch_identifiers_for_non_date_part_splitters(
        self,
        execution_engine: "SqlAlchemyExecutionEngine",  # noqa: F821
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
        except ValueError as e:
            raise ge_exceptions.InvalidConfigError(
                f"Please provide a supported splitter method name, you provided: {splitter_method_name}"
            )

    def _get_params_for_batch_identifiers_from_non_date_part_splitters(
        self,
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

    def get_split_query_for_data_for_batch_identifiers_for_split_on_whole_table(
        self,
        table_name: str,
    ) -> Selectable:
        """
        'Split' by returning the whole table

        Note: the table_name parameter is a required to keep the signature of this method consistent with other methods.
        """
        return sa.select([sa.true()])

    def get_split_query_for_data_for_batch_identifiers_for_split_on_column_value(
        self,
        table_name: str,
        column_name: str,
    ) -> Selectable:
        """Split using the values in the named column"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

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
        # query = f"SELECT DISTINCT( strftime(\"{date_format_string}\", \"{self.column_name}\")) as my_var FROM {self.table_name}"

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

    def get_split_query_for_data_for_batch_identifiers_for_split_on_divided_integer(
        self, table_name: str, column_name: str, divisor: int
    ) -> Selectable:
        """Divide the values in the named column by `divisor`, and split on that"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select(
            [sa.func.distinct(sa.cast(sa.column(column_name) / divisor, sa.Integer))]
        ).select_from(sa.text(table_name))

    def get_split_query_for_data_for_batch_identifiers_for_split_on_mod_integer(
        self, table_name: str, column_name: str, mod: int
    ) -> Selectable:
        """Divide the values in the named column by `divisor`, and split on that"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select(
            [sa.func.distinct(sa.cast(sa.column(column_name) % mod, sa.Integer))]
        ).select_from(sa.text(table_name))

    def get_split_query_for_data_for_batch_identifiers_for_split_on_multi_column_values(
        self,
        table_name: str,
        column_names: List[str],
    ) -> Selectable:
        """Split on the joint values in the named columns"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

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
        # query = f"SELECT MD5(\"{self.column_name}\") = {matching_hash}) AS hashed_var FROM {self.table_name}"

        return sa.select([sa.func.md5(sa.column(column_name))]).select_from(
            sa.text(table_name)
        )
