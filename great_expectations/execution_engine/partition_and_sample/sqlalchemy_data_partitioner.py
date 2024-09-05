"""Create queries for use in sql data partitioning.

This module contains utilities for generating queries used by execution engines
and data connectors to partition data into batches based on the data itself. It
is typically used from within either an execution engine or a data connector,
not by itself.

    Typical usage example:
        __init__():
            self._sqlalchemy_data_partitioner = SqlAlchemyDataPartitioner()

        elsewhere():
            partitioner = self._sqlalchemy_data_partitioner._get_partitioner_method()
            partition_query_or_clause = partitioner()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DataPartitioner,
    DatePart,
    PartitionerMethod,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.execution_engine.sqlalchemy_execution_engine import (
        SqlAlchemyExecutionEngine,
    )


class SqlAlchemyDataPartitioner(DataPartitioner):
    """Methods for partitioning data accessible via SqlAlchemyExecutionEngine.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. SqlAlchemyDataPartitioner.date_part.MONTH
    """

    def __init__(self, dialect: str):
        self._dialect = dialect

    DATETIME_PARTITIONER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        PartitionerMethod.PARTITION_ON_YEAR: "get_data_for_batch_identifiers_year",
        PartitionerMethod.PARTITION_ON_YEAR_AND_MONTH: "get_data_for_batch_identifiers_year_and_month",  # noqa: E501
        PartitionerMethod.PARTITION_ON_YEAR_AND_MONTH_AND_DAY: "get_data_for_batch_identifiers_year_and_month_and_day",  # noqa: E501
        PartitionerMethod.PARTITION_ON_DATE_PARTS: "get_data_for_batch_identifiers_for_partition_on_date_parts",  # noqa: E501
    }

    PARTITIONER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING: dict = {
        PartitionerMethod.PARTITION_ON_WHOLE_TABLE: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_whole_table",  # noqa: E501
        PartitionerMethod.PARTITION_ON_COLUMN_VALUE: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_column_value",  # noqa: E501
        PartitionerMethod.PARTITION_ON_CONVERTED_DATETIME: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_converted_datetime",  # noqa: E501
        PartitionerMethod.PARTITION_ON_DIVIDED_INTEGER: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_divided_integer",  # noqa: E501
        PartitionerMethod.PARTITION_ON_MOD_INTEGER: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_mod_integer",  # noqa: E501
        PartitionerMethod.PARTITION_ON_MULTI_COLUMN_VALUES: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_multi_column_values",  # noqa: E501
        PartitionerMethod.PARTITION_ON_HASHED_COLUMN: "get_partition_query_for_data_for_batch_identifiers_for_partition_on_hashed_column",  # noqa: E501
    }

    def partition_on_year(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> Union[sqlalchemy.BinaryExpression, sqlalchemy.BooleanClauseList]:
        """Partition on year values in column_name.

        Args:
            column_name: column in table to use in determining partition.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for partitioning or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.partition_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR],
        )

    def partition_on_year_and_month(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> Union[sqlalchemy.BinaryExpression, sqlalchemy.BooleanClauseList]:
        """Partition on year and month values in column_name.

        Args:
            column_name: column in table to use in determining partition.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for partitioning or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.partition_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH],
        )

    def partition_on_year_and_month_and_day(
        self,
        column_name: str,
        batch_identifiers: dict,
    ) -> Union[sqlalchemy.BinaryExpression, sqlalchemy.BooleanClauseList]:
        """Partition on year and month and day values in column_name.

        Args:
            column_name: column in table to use in determining partition.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for partitioning or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.partition_on_date_parts(
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        )

    def partition_on_date_parts(
        self,
        column_name: str,
        batch_identifiers: dict,
        date_parts: Union[List[DatePart], List[str]],
    ) -> Union[sqlalchemy.BinaryExpression, sqlalchemy.BooleanClauseList]:
        """Partition on date_part values in column_name.

        Values are NOT truncated, for example this will return data for a
        given month (if only month is chosen for date_parts) for ALL years.
        This may be useful for viewing seasonality, but you can also specify
        multiple date_parts to achieve date_trunc like behavior e.g.
        year, month and day.

        Args:
            column_name: column in table to use in determining partition.
            batch_identifiers: should contain a dateutil parseable datetime whose date parts
                will be used for partitioning or key values of {date_part: date_part_value}
            date_parts: part of the date to be used for partitioning e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        self._validate_date_parts(date_parts)

        date_parts = self._convert_date_parts(date_parts)

        column_batch_identifiers: dict = batch_identifiers[column_name]

        date_parts_dict: dict = self._convert_datetime_batch_identifiers_to_date_parts_dict(
            column_batch_identifiers, date_parts
        )

        query: Union[sqlalchemy.BinaryExpression, sqlalchemy.BooleanClauseList] = sa.and_(  # type: ignore[assignment]
            *[
                sa.extract(date_part.value, sa.column(column_name))
                == date_parts_dict[date_part.value]
                for date_part in date_parts
            ]
        )

        return query

    @staticmethod
    def partition_on_whole_table(batch_identifiers: dict) -> bool:
        """'Partition' by returning the whole table"""

        return True

    @staticmethod
    def partition_on_column_value(column_name: str, batch_identifiers: dict) -> bool:
        """Partition using the values in the named column"""

        return sa.column(column_name) == batch_identifiers[column_name]

    def partition_on_converted_datetime(
        self,
        column_name: str,
        batch_identifiers: dict,
        date_format_string: str = "%Y-%m-%d",
    ) -> bool:
        """Convert the values in the named column to the given date_format, and partition on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.func.strftime(
                    date_format_string,
                    sa.column(column_name),
                )
                == batch_identifiers[column_name]
            )

        raise NotImplementedError(
            f'Partitioner method "partition_on_converted_datetime" is not supported for "{self._dialect}" SQL dialect.'  # noqa: E501
        )

    def partition_on_divided_integer(
        self,
        column_name: str,
        divisor: int,
        batch_identifiers: dict,
    ) -> bool:
        """Divide the values in the named column by `divisor`, and partition on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.cast((sa.cast(sa.column(column_name), sa.Integer) / divisor), sa.Integer)
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.MYSQL:
            return (
                sa.cast(
                    sa.func.truncate((sa.cast(sa.column(column_name), sa.Integer) / divisor), 0),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.MSSQL:
            return (
                sa.cast(
                    sa.func.round((sa.cast(sa.column(column_name), sa.Integer) / divisor), 0, 1),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        if self._dialect == GXSqlDialect.AWSATHENA:
            return (
                sa.cast(
                    sa.func.truncate(sa.cast(sa.column(column_name), sa.Integer) / divisor),
                    sa.Integer,
                )
                == batch_identifiers[column_name]
            )

        return (
            sa.cast(
                sa.func.trunc((sa.cast(sa.column(column_name), sa.Integer) / divisor), 0),
                sa.Integer,
            )
            == batch_identifiers[column_name]
        )

    def partition_on_mod_integer(
        self,
        column_name: str,
        mod: int,
        batch_identifiers: dict,
    ) -> bool:
        """Divide the values in the named column by `mod`, and partition on that"""
        if self._dialect in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.MSSQL,
        ]:
            return (
                sa.cast(sa.column(column_name), sa.Integer) % mod == batch_identifiers[column_name]
            )

        return (
            sa.func.mod(sa.cast(sa.column(column_name), sa.Integer), mod)
            == batch_identifiers[column_name]
        )

    @staticmethod
    def partition_on_multi_column_values(
        column_names: List[str],
        batch_identifiers: dict,
    ) -> bool:
        """Partition on the joint values in the named columns"""

        return sa.and_(  # type: ignore[return-value]
            *(
                sa.column(column_name) == column_value
                for column_name, column_value in batch_identifiers.items()
            )
        )

    def partition_on_hashed_column(
        self,
        column_name: str,
        hash_digits: int,
        batch_identifiers: dict,
    ) -> bool:
        """Partition on the hashed value of the named column"""
        if self._dialect == GXSqlDialect.SQLITE:
            return (
                sa.func.md5(sa.cast(sa.column(column_name), sa.VARCHAR), hash_digits)
                == batch_identifiers[column_name]
            )

        raise NotImplementedError(
            f'Partitioner method "partition_on_hashed_column" is not supported for "{self._dialect}" SQL dialect.'  # noqa: E501
        )

    def get_data_for_batch_identifiers(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        partitioner_method_name: str,
        partitioner_kwargs: dict,
    ) -> List[dict]:
        """Build data used to construct batch identifiers for the input table using the provided partitioner config.

        Sql partitioner configurations yield the unique values that comprise a batch by introspecting your data.

        Args:
            execution_engine: Used to introspect the data.
            selectable: Selectable to partition.
            partitioner_method_name: Desired partitioner method to use.
            partitioner_kwargs: Dict of directives used by the partitioner method as keyword arguments of key=value.

        Returns:
            List of dicts of the form [{column_name: {"key": value}}]
        """  # noqa: E501
        processed_partitioner_method_name: str = self._get_partitioner_method_name(
            partitioner_method_name
        )
        batch_identifiers_list: List[dict]
        if self._is_datetime_partitioner(processed_partitioner_method_name):
            partitioner_fn_name: str = (
                self.DATETIME_PARTITIONER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING[
                    processed_partitioner_method_name
                ]
            )
            batch_identifiers_list = getattr(self, partitioner_fn_name)(
                execution_engine, selectable, **partitioner_kwargs
            )
        else:
            batch_identifiers_list = (
                self.get_data_for_batch_identifiers_for_non_date_part_partitioners(
                    execution_engine,
                    selectable,
                    processed_partitioner_method_name,
                    partitioner_kwargs,
                )
            )

        return batch_identifiers_list

    def _is_datetime_partitioner(self, partitioner_method_name: str) -> bool:
        """Whether the partitioner method is a datetime partitioner.

        Args:
            partitioner_method_name: Name of the partitioner method

        Returns:
            Boolean
        """
        return partitioner_method_name in list(
            self.DATETIME_PARTITIONER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING.keys()
        )

    def get_data_for_batch_identifiers_year(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column partition on year.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            selectable: selectable to partition.
            column_name: column in table to use in determining partition.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022}}]
        """
        return self.get_data_for_batch_identifiers_for_partition_on_date_parts(
            execution_engine=execution_engine,
            selectable=selectable,
            column_name=column_name,
            date_parts=[DatePart.YEAR],
        )

    def get_data_for_batch_identifiers_year_and_month(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column partition on year and month.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            selectable: selectable to partition.
            column_name: column in table to use in determining partition.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022, "month": 4}}]
        """
        return self.get_data_for_batch_identifiers_for_partition_on_date_parts(
            execution_engine=execution_engine,
            selectable=selectable,
            column_name=column_name,
            date_parts=[DatePart.YEAR, DatePart.MONTH],
        )

    def get_data_for_batch_identifiers_year_and_month_and_day(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        column_name: str,
    ) -> List[dict]:
        """Build batch_identifiers from a column partition on year and month and day.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            selectable: selectable to partition.
            column_name: column in table to use in determining partition.

        Returns:
            List of dicts of the form [{column_name: {"year": 2022, "month": 4, "day": 14}}]
        """
        return self.get_data_for_batch_identifiers_for_partition_on_date_parts(
            execution_engine=execution_engine,
            selectable=selectable,
            column_name=column_name,
            date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        )

    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(
        self,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        date_parts: Union[List[DatePart], List[str]],
    ) -> sqlalchemy.Selectable:
        """Build a partition query to retrieve batch_identifiers info from a column partition on a list of date parts.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            selectable: selectable to partition.
            column_name: column in table to use in determining partition.
            date_parts: part of the date to be used for partitioning e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """  # noqa: E501
        self._validate_date_parts(date_parts)

        date_parts = self._convert_date_parts(date_parts)

        # NOTE: AJB 20220414 concatenating to find distinct values to support all dialects.
        # There are more performant dialect-specific methods that can be implemented in
        # future improvements.
        # NOTE: AJB 20220511 for awsathena we need to cast extracted date parts
        # to string first before concatenating them.
        concat_clause: list[sqlalchemy.Label]
        concat_date_parts: sqlalchemy.Cast | sqlalchemy.ColumnOperators
        if len(date_parts) == 1:
            # MSSql does not accept single item concatenation
            concat_clause = sa.func.distinct(  # type: ignore[assignment]
                sa.func.extract(date_parts[0].value, sa.column(column_name)).label(
                    date_parts[0].value
                )
            ).label("concat_distinct_values")

        else:
            """
            # NOTE: <Alex>6/29/2022</Alex>
            Certain SQLAlchemy-compliant backends (e.g., Amazon Redshift, SQLite) allow only binary operators for "CONCAT".
            """  # noqa: E501
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

                concat_clause = sa.func.distinct(concat_date_parts).label("concat_distinct_values")  # type: ignore[assignment]
            else:
                concat_date_parts = sa.func.concat(
                    "",
                    sa.cast(
                        sa.func.extract(date_parts[0].value, sa.column(column_name)),
                        sa.String,
                    ),
                )

                for date_part in date_parts[1:]:
                    concat_date_parts = sa.func.concat(
                        concat_date_parts,
                        sa.cast(
                            sa.func.extract(date_part.value, sa.column(column_name)),
                            sa.String,
                        ),
                    )

                concat_clause = sa.func.distinct(concat_date_parts).label("concat_distinct_values")  # type: ignore[assignment]

        partitioned_query: sqlalchemy.Selectable = sa.select(  # type: ignore[call-overload]
            concat_clause,
            *[
                sa.cast(sa.func.extract(date_part.value, sa.column(column_name)), sa.Integer).label(
                    date_part.value
                )
                for date_part in date_parts
            ],
        ).select_from(selectable)

        return partitioned_query

    def get_data_for_batch_identifiers_for_partition_on_date_parts(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        date_parts: Union[List[DatePart], List[str]],
    ) -> List[dict]:
        """Build batch_identifiers from a column partition on a list of date parts.

        This method builds a query to select the unique date_parts from the
        column_name. This data can be used to build BatchIdentifiers.

        Args:
            execution_engine: used to query the data to find batch identifiers.
            selectable: selectable to partition.
            column_name: column in table to use in determining partition.
            date_parts: part of the date to be used for partitioning e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """

        partitioned_query: sqlalchemy.Selectable = (
            self.get_partition_query_for_data_for_batch_identifiers_for_partition_on_date_parts(
                selectable, column_name, date_parts
            )
        )

        result: List[sqlalchemy.Row | sqlalchemy.LegacyRow] = self._execute_partitioned_query(
            execution_engine, partitioned_query
        )

        return self._get_params_for_batch_identifiers_from_date_part_partitioner(
            column_name, result, date_parts
        )

    @staticmethod
    def _execute_partitioned_query(
        execution_engine: SqlAlchemyExecutionEngine,
        partitioned_query: sqlalchemy.Selectable,
    ) -> List[sqlalchemy.Row | sqlalchemy.LegacyRow]:
        """Use the provided execution engine to run the partition query and fetch all of the results.

        Args:
            execution_engine: SqlAlchemyExecutionEngine to be used for executing the query.
            partitioned_query: Query to be executed as a sqlalchemy Selectable.

        Returns:
            List of row results.
        """  # noqa: E501
        return execution_engine.execute_partitioned_query(partitioned_query)

    def _get_params_for_batch_identifiers_from_date_part_partitioner(
        self,
        column_name: str,
        result: List[sqlalchemy.LegacyRow],
        date_parts: List[DatePart] | List[str],
    ) -> List[dict]:
        """Get parameters used to build BatchIdentifiers from the results of a get_data_for_batch_identifiers_for_partition_on_date_parts

        Args:
            column_name: Column name associated with the batch identifier.
            result: list of LegacyRow objects from sqlalchemy query result.
            date_parts: part of the date to be used for constructing the batch identifiers e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            List of dicts of the form [{column_name: {date_part_name: date_part_value}}]
        """  # noqa: E501
        date_parts = self._convert_date_parts(date_parts)

        data_for_batch_identifiers: List[dict] = [
            {
                column_name: {
                    date_part.value: getattr(row, date_part.value) for date_part in date_parts
                }
            }
            for row in result
        ]
        return data_for_batch_identifiers

    @staticmethod
    def _get_column_names_from_partitioner_kwargs(partitioner_kwargs) -> List[str]:
        column_names: List[str] = []

        if "column_names" in partitioner_kwargs:
            column_names = partitioner_kwargs["column_names"]
        elif "column_name" in partitioner_kwargs:
            column_names = [partitioner_kwargs["column_name"]]

        return column_names

    def get_data_for_batch_identifiers_for_non_date_part_partitioners(
        self,
        execution_engine: SqlAlchemyExecutionEngine,
        selectable: sqlalchemy.Selectable,
        partitioner_method_name: str,
        partitioner_kwargs: dict,
    ) -> List[dict]:
        """Build data used to construct batch identifiers for the input table using the provided partitioner config.

        Sql partitioner configurations yield the unique values that comprise a batch by introspecting your data.

        Args:
            execution_engine: Used to introspect the data.
            selectable: selectable to partition.
            partitioner_method_name: Desired partitioner method to use.
            partitioner_kwargs: Dict of directives used by the partitioner method as keyword arguments of key=value.

        Returns:
            List of dicts of the form [{column_name: {"key": value}}]
        """  # noqa: E501
        get_partition_query_method_name: str = (
            self._get_method_name_for_get_data_for_batch_identifiers_method(partitioner_method_name)
        )

        partitioned_query: sqlalchemy.Selectable = getattr(self, get_partition_query_method_name)(
            selectable=selectable, **partitioner_kwargs
        )
        rows: List[sqlalchemy.LegacyRow] = self._execute_partitioned_query(
            execution_engine, partitioned_query
        )
        column_names: List[str] = self._get_column_names_from_partitioner_kwargs(partitioner_kwargs)
        return self._get_params_for_batch_identifiers_from_non_date_part_partitioners(
            column_names, rows
        )

    def _get_method_name_for_get_data_for_batch_identifiers_method(
        self, partitioner_method_name: str
    ):
        """Get the matching method name to get the data for batch identifiers from the input partitioner method name.

        Args:
            partitioner_method_name: Configured partitioner name.

        Returns:
            Name of the corresponding method to get data for building batch identifiers.
        """  # noqa: E501
        processed_partitioner_method_name: str = self._get_partitioner_method_name(
            partitioner_method_name
        )
        try:
            return self.PARTITIONER_METHOD_TO_GET_UNIQUE_BATCH_IDENTIFIERS_METHOD_MAPPING[
                processed_partitioner_method_name
            ]
        except ValueError:
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"Please provide a supported partitioner method name, you provided: {partitioner_method_name}"  # noqa: E501
            )

    @staticmethod
    def _get_params_for_batch_identifiers_from_non_date_part_partitioners(
        column_names: List[str],
        rows: List[sqlalchemy.LegacyRow],
    ) -> List[dict]:
        """Get params used in batch identifiers from output of executing query for non date part partitioners.

        Args:
            column_names: Column names referenced in partitioner_kwargs.
            rows: Rows from execution of partition query.

        Returns:
            Dict of {column_name: row, column_name: row, ...}
        """  # noqa: E501
        return [dict(zip(column_names, row)) for row in rows]

    @staticmethod
    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_whole_table(
        selectable: sqlalchemy.Selectable,
    ) -> sqlalchemy.Selectable:
        """
        'Partition' by returning the whole table

        Note: the selectable parameter is a required to keep the signature of this method consistent with other methods.
        """  # noqa: E501
        return sa.select(sa.true())

    @staticmethod
    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_column_value(
        selectable: sqlalchemy.Selectable,
        column_name: str,
    ) -> sqlalchemy.Selectable:
        """Partition using the values in the named column"""
        return (
            sa.select(sa.func.distinct(sa.column(column_name)))
            .select_from(selectable)  # type: ignore[arg-type]
            .order_by(sa.column(column_name).asc())
        )

    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_converted_datetime(
        self,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        date_format_string: str = "%Y-%m-%d",
    ) -> sqlalchemy.Selectable:
        """Convert the values in the named column to the given date_format, and partition on that"""
        if self._dialect == "sqlite":
            return sa.select(
                sa.func.distinct(
                    sa.func.strftime(
                        date_format_string,
                        sa.column(column_name),
                    )
                )
            ).select_from(selectable)  # type: ignore[arg-type]

        raise NotImplementedError(
            f'Partitioner method "partition_on_converted_datetime" is not supported for "{self._dialect}" SQL dialect.'  # noqa: E501
        )

    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_divided_integer(
        self,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        divisor: int,
    ) -> sqlalchemy.Selectable:
        """Divide the values in the named column by `divisor`, and partition on that"""
        if self._dialect == GXSqlDialect.SQLITE:
            return sa.select(
                sa.func.distinct(
                    sa.cast(
                        (sa.cast(sa.column(column_name), sa.Integer) / divisor),
                        sa.Integer,
                    )
                )
            ).select_from(selectable)  # type: ignore[arg-type]

        if self._dialect == GXSqlDialect.MYSQL:
            return sa.select(
                sa.func.distinct(
                    sa.cast(
                        sa.func.truncate(
                            (sa.cast(sa.column(column_name), sa.Integer) / divisor),
                            0,
                        ),
                        sa.Integer,
                    )
                )
            ).select_from(selectable)  # type: ignore[arg-type]

        if self._dialect == GXSqlDialect.MSSQL:
            return sa.select(
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
            ).select_from(selectable)  # type: ignore[arg-type]

        if self._dialect == GXSqlDialect.AWSATHENA:
            return sa.select(
                sa.func.distinct(
                    sa.cast(
                        sa.func.truncate(sa.cast(sa.column(column_name), sa.Integer) / divisor),
                        sa.Integer,
                    )
                )
            ).select_from(selectable)  # type: ignore[arg-type]

        return sa.select(
            sa.func.distinct(
                sa.cast(
                    sa.func.trunc((sa.cast(sa.column(column_name), sa.Integer) / divisor), 0),
                    sa.Integer,
                )
            )
        ).select_from(selectable)  # type: ignore[arg-type]

    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_mod_integer(
        self,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        mod: int,
    ) -> sqlalchemy.Selectable:
        """Divide the values in the named column by `mod`, and partition on that"""
        if self._dialect in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.MSSQL,
        ]:
            return sa.select(
                sa.func.distinct(sa.cast(sa.column(column_name), sa.Integer) % mod)
            ).select_from(selectable)  # type: ignore[arg-type]

        return sa.select(
            sa.func.distinct(sa.func.mod(sa.cast(sa.column(column_name), sa.Integer), mod))
        ).select_from(selectable)  # type: ignore[arg-type]

    @staticmethod
    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_multi_column_values(
        selectable: sqlalchemy.Selectable,
        column_names: List[str],
    ) -> sqlalchemy.Selectable:
        """Partition on the joint values in the named columns"""
        return (
            sa.select(*[sa.column(column_name) for column_name in column_names])
            .distinct()
            .select_from(selectable)  # type: ignore[arg-type]
        )

    def get_partition_query_for_data_for_batch_identifiers_for_partition_on_hashed_column(
        self,
        selectable: sqlalchemy.Selectable,
        column_name: str,
        hash_digits: int,
    ) -> sqlalchemy.Selectable:
        """Note: this method is experimental. It does not work with all SQL dialects."""
        if self._dialect == GXSqlDialect.SQLITE:
            return sa.select(
                sa.func.distinct(
                    sa.func.md5(sa.cast(sa.column(column_name), sa.VARCHAR), hash_digits)
                )
            ).select_from(selectable)  # type: ignore[arg-type]

        raise NotImplementedError(
            f'Partitioner method "partition_on_hashed_column" is not supported for "{self._dialect}" SQL dialect.'  # noqa: E501
        )
