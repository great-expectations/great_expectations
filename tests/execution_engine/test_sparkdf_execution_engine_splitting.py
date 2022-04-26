from typing import List, Tuple

import pytest

from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart

try:
    pyspark = pytest.importorskip("pyspark")
    # noinspection PyPep8Naming
    import pyspark.sql.functions as F
except ImportError:
    pyspark = None
    F = None


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
    simple_multi_year_spark_df,
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
    simple_multi_year_spark_df,
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
