from __future__ import annotations

import hashlib
import logging
from typing import List, Union

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.exceptions import exceptions as gx_exceptions
from great_expectations.execution_engine.split_and_sample.data_splitter import (
    DataSplitter,
    DatePart,
)

logger = logging.getLogger(__name__)


class SparkDataSplitter(DataSplitter):
    """Methods for splitting data accessible via SparkDFExecutionEngine.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. SparkDataSplitter.date_part.MONTH
    """

    def split_on_year(
        self,
        df: pyspark.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.DataFrame:
        """Split on year values in column_name.

        Args:
            df: dataframe from batch data.
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            df=df,
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR],
        )

    def split_on_year_and_month(
        self,
        df: pyspark.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.DataFrame:
        """Split on year and month values in column_name.

        Args:
            df: dataframe from batch data.
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            df=df,
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH],
        )

    def split_on_year_and_month_and_day(
        self,
        df: pyspark.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.DataFrame:
        """Split on year and month and day values in column_name.

        Args:
            df: dataframe from batch data.
            column_name: column in table to use in determining split.
            batch_identifiers: should contain a dateutil parseable datetime whose
                relevant date parts will be used for splitting or key values
                of {date_part: date_part_value}.

        Returns:
            List of boolean clauses based on whether the date_part value in the
                batch identifier matches the date_part value in the column_name column.
        """
        return self.split_on_date_parts(
            df=df,
            column_name=column_name,
            batch_identifiers=batch_identifiers,
            date_parts=[DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        )

    def split_on_date_parts(
        self,
        df: pyspark.DataFrame,
        column_name: str,
        batch_identifiers: dict,
        date_parts: Union[List[DatePart], List[str]],
    ) -> pyspark.DataFrame:
        """Split on date_part values in column_name.

        Values are NOT truncated, for example this will return data for a
        given month (if only month is chosen for date_parts) for ALL years.
        This may be useful for viewing seasonality, but you can also specify
        multiple date_parts to achieve date_trunc like behavior e.g.
        year, month and day.

        Args:
            df: dataframe from batch data.
            column_name: column in data used to determine split.
            batch_identifiers: should contain a dateutil parseable datetime whose date parts
                will be used for splitting or key values of {date_part: date_part_value}
            date_parts: part of the date to be used for splitting e.g.
                DatePart.DAY or the case-insensitive string representation "day"

        Returns:
            Dataframe with splitting applied.
        """
        self._validate_date_parts(date_parts)

        date_parts = self._convert_date_parts(date_parts)

        column_batch_identifiers: dict = batch_identifiers[column_name]

        date_parts_dict: dict = (
            self._convert_datetime_batch_identifiers_to_date_parts_dict(
                column_batch_identifiers, date_parts
            )
        )

        for date_part, date_part_value in date_parts_dict.items():
            df = df.filter(
                getattr(F, self._convert_date_part_to_spark_equivalent(date_part))(
                    F.col(column_name)
                )
                == date_part_value
            )
        return df

    @staticmethod
    def _convert_date_part_to_spark_equivalent(date_part: DatePart | str) -> str:
        """Convert the DatePart to a string representing the corresponding pyspark.sql.functions version.

        For example DatePart.DAY -> pyspark.sql.functions.dayofmonth() -> "dayofmonth"

        Args:
            date_part: DatePart representing the part of the datetime to extract or string equivalent.

        Returns:
            String representing the spark function to use for the given DatePart.
        """
        date_part = DatePart(date_part)

        spark_date_part_decoder: dict = {
            DatePart.YEAR: "year",
            DatePart.MONTH: "month",
            DatePart.WEEK: "weekofyear",
            DatePart.DAY: "dayofmonth",
            DatePart.HOUR: "hour",
            DatePart.MINUTE: "minute",
            DatePart.SECOND: "second",
        }
        return spark_date_part_decoder[date_part]

    @staticmethod
    def split_on_whole_table(
        df: pyspark.DataFrame,
    ) -> pyspark.DataFrame:
        """No op. Return the same data that is passed in.

        Args:
            df: Spark DataFrame that will be returned

        Returns:
            Unfiltered DataFrame.
        """
        return df

    @staticmethod
    def split_on_column_value(
        df, column_name: str, batch_identifiers: dict
    ) -> pyspark.DataFrame:
        """Return a dataframe where rows are filtered based on the specified column value.

        Args:
            df: Spark DataFrame to be filtered.
            column_name: Column to use in comparison.
            batch_identifiers: Contains value to use in comparison e.g. batch_identifiers={ 'col': value }.

        Returns:
            Filtered spark DataFrame.
        """
        return df.filter(F.col(column_name) == batch_identifiers[column_name])

    @staticmethod
    def split_on_converted_datetime(
        df,
        column_name: str,
        batch_identifiers: dict,
        date_format_string: str = "yyyy-MM-dd",
    ) -> pyspark.DataFrame:
        """Return a dataframe where rows are filtered based on whether their converted
        datetime (using date_format_string) matches the datetime string value provided
        in batch_identifiers for the specified column.

        Args:
            df: Spark DataFrame to be filtered.
            column_name: Column to use in comparison.
            batch_identifiers: Value to use in comparison as {column_name: datetime string}.
            date_format_string: Format used to convert datetime column for comparison to
                batch identifiers.

        Returns:
            Filtered spark DataFrame.
        """
        matching_string = batch_identifiers[column_name]
        res = (
            df.withColumn(
                "date_time_tmp", F.from_unixtime(F.col(column_name), date_format_string)
            )
            .filter(F.col("date_time_tmp") == matching_string)
            .drop("date_time_tmp")
        )
        return res

    @staticmethod
    def split_on_divided_integer(
        df, column_name: str, divisor: int, batch_identifiers: dict
    ):
        """Divide the values in the named column by `divisor`, and split on that"""
        matching_divisor = batch_identifiers[column_name]
        res = (
            df.withColumn(
                "div_temp",
                (F.col(column_name) / divisor).cast(pyspark.types.IntegerType()),
            )
            .filter(F.col("div_temp") == matching_divisor)
            .drop("div_temp")
        )
        return res

    @staticmethod
    def split_on_mod_integer(df, column_name: str, mod: int, batch_identifiers: dict):
        """Divide the values in the named column by `divisor`, and split on that"""
        matching_mod_value = batch_identifiers[column_name]
        res = (
            df.withColumn(
                "mod_temp", (F.col(column_name) % mod).cast(pyspark.types.IntegerType())
            )
            .filter(F.col("mod_temp") == matching_mod_value)
            .drop("mod_temp")
        )
        return res

    @staticmethod
    def split_on_multi_column_values(df, column_names: list, batch_identifiers: dict):
        """Split on the joint values in the named columns"""
        for column_name in column_names:
            value = batch_identifiers.get(column_name)
            if not value:
                raise ValueError(
                    f"In order for SparkDFExecutionEngine to `_split_on_multi_column_values`, "
                    f"all values in  column_names must also exist in batch_identifiers. "
                    f"{column_name} was not found in batch_identifiers."
                )
            df = df.filter(F.col(column_name) == value)
        return df

    @staticmethod
    def split_on_hashed_column(
        df,
        column_name: str,
        hash_digits: int,
        batch_identifiers: dict,
        hash_function_name: str = "sha256",
    ):
        """Split on the hashed value of the named column"""
        try:
            getattr(hashlib, hash_function_name)
        except (TypeError, AttributeError):
            raise (
                gx_exceptions.ExecutionEngineError(
                    f"""The splitting method used with SparkDFExecutionEngine has a reference to an invalid hash_function_name.
                    Reference to {hash_function_name} cannot be found."""
                )
            )

        def _encrypt_value(to_encode):
            hash_func = getattr(hashlib, hash_function_name)
            hashed_value = hash_func(to_encode.encode()).hexdigest()[-1 * hash_digits :]
            return hashed_value

        encrypt_udf = F.udf(_encrypt_value, pyspark.types.StringType())
        res = (
            df.withColumn("encrypted_value", encrypt_udf(column_name))
            .filter(F.col("encrypted_value") == batch_identifiers["hash_value"])
            .drop("encrypted_value")
        )
        return res
