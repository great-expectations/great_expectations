from typing import List, Tuple
from unittest import mock

import pytest

from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart
from great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter import (
    SparkDataSplitter,
)

try:
    pyspark = pytest.importorskip("pyspark")
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrame

except ImportError:
    pyspark = None
    F = None
    DataFrame = None


@pytest.fixture
def simple_multi_year_spark_df(spark_session):
    spark_df_data: List[Tuple] = [
        ("2018-01-01 12:00:00.000",),
        ("2018-10-02 12:00:00.000",),
        ("2019-01-01 12:00:00.000",),
        ("2019-10-02 12:00:00.000",),
        ("2019-11-03 12:00:00.000",),
        ("2020-01-01 12:00:00.000",),
        ("2020-10-02 12:00:00.000",),
        ("2020-11-03 12:00:00.000",),
        ("2020-12-04 12:00:00.000",),
    ]

    spark_df: pyspark.sql.DataFrame = spark_session.createDataFrame(
        data=spark_df_data, schema=["input_timestamp"]
    )
    spark_df = spark_df.withColumn("timestamp", F.to_timestamp("input_timestamp"))
    assert spark_df.count() == 9
    return spark_df


@pytest.mark.integration
@pytest.mark.parametrize(
    "splitter_kwargs_year,num_values_in_df",
    [
        pytest.param(year, num_values, id=year)
        for year, num_values in {"2018": 2, "2019": 3, "2020": 4}.items()
    ],
)
def test_get_batch_with_split_on_year(
    splitter_kwargs_year,
    num_values_in_df,
    spark_session,
    basic_spark_df_execution_engine,
    simple_multi_year_spark_df: DataFrame,
):

    split_df: pyspark.sql.DataFrame = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_spark_df,
            splitter_method="split_on_year",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": splitter_kwargs_year},
            },
        )
    ).dataframe
    assert split_df.count() == num_values_in_df
    assert len(split_df.columns) == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    "column_batch_identifier,num_values_in_df",
    [
        pytest.param(column_batch_identifier, num_values, id=column_batch_identifier)
        for column_batch_identifier, num_values in {
            "2018-01-01": 3,
            "2018-01-02": 3,
            "2018-01-03": 2,
            "2018-01-04": 1,
        }.items()
    ],
)
def test_get_batch_with_split_on_date_parts(
    column_batch_identifier,
    num_values_in_df,
    spark_session,
    basic_spark_df_execution_engine,
    simple_multi_year_spark_df: DataFrame,
):

    split_df: pyspark.sql.DataFrame = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=simple_multi_year_spark_df,
            splitter_method="split_on_date_parts",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": column_batch_identifier},
                "date_parts": [DatePart.DAY],
            },
        )
    ).dataframe

    assert split_df.count() == num_values_in_df
    assert len(split_df.columns) == 2


# TODO: AJB 20220426 Test with different style batch_identifiers (str, datetime)
# TODO: AJB 20220426 Test with unrelated batch_identifiers
# TODO: AJB 20220426 Test with multiple dateparts, mix of dateparts (use just splitter for this?)


@mock.patch(
    "great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter.SparkDataSplitter.split_on_date_parts"
)
@pytest.mark.parametrize(
    "splitter_method_name,called_with_date_parts",
    [
        ("split_on_year", [DatePart.YEAR]),
        ("split_on_year_and_month", [DatePart.YEAR, DatePart.MONTH]),
        (
            "split_on_year_and_month_and_day",
            [DatePart.YEAR, DatePart.MONTH, DatePart.DAY],
        ),
    ],
)
def test_named_date_part_methods(
    mock_split_on_date_parts: mock.MagicMock,
    splitter_method_name: str,
    called_with_date_parts: List[DatePart],
    simple_multi_year_spark_df: DataFrame,
):
    """Test that a partially pre-filled version of split_on_date_parts() was called with the appropriate params.
    For example, split_on_year.
    """
    data_splitter: SparkDataSplitter = SparkDataSplitter()
    column_name: str = "column_name"
    batch_identifiers: dict = {column_name: {"year": 2018, "month": 10, "day": 31}}

    getattr(data_splitter, splitter_method_name)(
        df=simple_multi_year_spark_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
    )

    mock_split_on_date_parts.assert_called_with(
        df=simple_multi_year_spark_df,
        column_name=column_name,
        batch_identifiers=batch_identifiers,
        date_parts=called_with_date_parts,
    )


@pytest.mark.parametrize(
    "underscore_prefix",
    [
        pytest.param("_", id="underscore prefix"),
        pytest.param("", id="no underscore prefix"),
    ],
)
@pytest.mark.parametrize(
    "splitter_method_name",
    [
        pytest.param(splitter_method_name, id=splitter_method_name)
        for splitter_method_name in [
            "split_on_year",
            "split_on_year_and_month",
            "split_on_year_and_month_and_day",
            "split_on_date_parts",
            "split_on_whole_table",
            "split_on_column_value",
            "split_on_converted_datetime",
            "split_on_divided_integer",
            "split_on_mod_integer",
            "split_on_multi_column_values",
            "split_on_hashed_column",
        ]
    ],
)
def test_get_splitter_method(underscore_prefix: str, splitter_method_name: str):
    data_splitter: SparkDataSplitter = SparkDataSplitter()

    splitter_method_name_with_prefix = f"{underscore_prefix}{splitter_method_name}"

    assert data_splitter.get_splitter_method(
        splitter_method_name_with_prefix
    ) == getattr(data_splitter, splitter_method_name)
