import logging
from typing import List, Union

from great_expectations.execution_engine.split_and_sample.data_splitter import (
    DataSplitter,
    DatePart,
)

logger = logging.getLogger(__name__)

try:
    import pyspark
    import pyspark.sql.functions as F
except ImportError:
    pyspark = None
    F = None
    logger.debug(
        "Unable to load pyspark; install optional spark dependency if you will be working with Spark dataframes"
    )


class SparkDataSplitter(DataSplitter):
    """Methods for splitting data accessible via SqlAlchemyExecutionEngine.

    Note, for convenience, you can also access DatePart via the instance variable
    date_part e.g. SparkDataSplitter.date_part.MONTH
    """

    def split_on_year(
        self,
        df: pyspark.sql.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.sql.DataFrame:
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
        df: pyspark.sql.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.sql.DataFrame:
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
        df: pyspark.sql.DataFrame,
        column_name: str,
        batch_identifiers: dict,
    ) -> pyspark.sql.DataFrame:
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
        df: pyspark.sql.DataFrame,
        column_name: str,
        batch_identifiers: dict,
        date_parts: Union[List[DatePart], List[str]],
    ) -> pyspark.sql.DataFrame:
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

        date_parts: List[DatePart] = self._convert_date_parts(date_parts)

        column_batch_identifiers: dict = batch_identifiers[column_name]

        date_parts_dict: dict = (
            self._convert_datetime_batch_identifiers_to_date_parts_dict(
                column_batch_identifiers, date_parts
            )
        )

        for date_part, date_part_value in date_parts_dict.items():
            df = df.filter(getattr(F, date_part)(F.col(column_name)) == date_part_value)
        return df
