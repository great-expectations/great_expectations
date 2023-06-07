import datetime
import os
from typing import List, Tuple
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyarrow, pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.batch_spec import (
    AzureBatchSpec,
    GCSBatchSpec,
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart
from great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter import (
    SparkDataSplitter,
)
from tests.execution_engine.split_and_sample.split_and_sample_test_cases import (
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
    MULTIPLE_DATE_PART_DATE_PARTS,
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
    SINGLE_DATE_PART_DATE_PARTS,
)

# Here we add SparkDataSplitter specific test cases to the generic test cases:
SINGLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [SparkDataSplitter.date_part.MONTH],
        id="month getting date parts from SparkDataSplitter.date_part",
    )
]
MULTIPLE_DATE_PART_DATE_PARTS += [
    pytest.param(
        [SparkDataSplitter.date_part.YEAR, SparkDataSplitter.date_part.MONTH],
        id="year_month getting date parts from SparkDataSplitter.date_part",
    )
]


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

    spark_df: pyspark.DataFrame = spark_session.createDataFrame(
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
    simple_multi_year_spark_df: pyspark.DataFrame,
):
    split_df: pyspark.DataFrame = basic_spark_df_execution_engine.get_batch_data(
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
def test_get_batch_with_split_on_date_parts_day(
    column_batch_identifier,
    num_values_in_df,
    spark_session,
    basic_spark_df_execution_engine,
    simple_multi_year_spark_df: pyspark.DataFrame,
):
    split_df: pyspark.DataFrame = basic_spark_df_execution_engine.get_batch_data(
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    SINGLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    SINGLE_DATE_PART_DATE_PARTS,
)
def test_split_on_date_parts_single_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_spark_df
):
    """What does this test and why?

    split_on_date_parts should still filter the correct rows from the input dataframe when passed a single element list
     date_parts that is a string, DatePart enum objects, mixed case string.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """
    data_splitter: SparkDataSplitter = SparkDataSplitter()
    column_name: str = "timestamp"
    result: pyspark.DataFrame = data_splitter.split_on_date_parts(
        df=simple_multi_year_spark_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert result.count() == 3


@pytest.mark.parametrize(
    "batch_identifiers_for_column",
    MULTIPLE_DATE_PART_BATCH_IDENTIFIERS,
)
@pytest.mark.parametrize(
    "date_parts",
    MULTIPLE_DATE_PART_DATE_PARTS,
)
def test_split_on_date_parts_multiple_date_parts(
    batch_identifiers_for_column, date_parts, simple_multi_year_spark_df
):
    """What does this test and why?

    split_on_date_parts should still filter the correct rows from the input dataframe when passed
     date parts that are strings, DatePart enum objects, a mixture and mixed case.
     To match our interface it should accept a dateutil parseable string as the batch identifier
     or a datetime and also fail when parameters are invalid.
    """
    data_splitter: SparkDataSplitter = SparkDataSplitter()
    column_name: str = "timestamp"
    result: pyspark.DataFrame = data_splitter.split_on_date_parts(
        df=simple_multi_year_spark_df,
        column_name=column_name,
        batch_identifiers={column_name: batch_identifiers_for_column},
        date_parts=date_parts,
    )
    assert result.count() == 1


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
    simple_multi_year_spark_df: pyspark.DataFrame,
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


def test_get_batch_empty_splitter(
    test_folder_connection_path_csv, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(  # noqa: PTH118
                test_folder_connection_path_csv, "test.csv"
            ),
            reader_options={"header": True},
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_empty_splitter_tsv(
    test_folder_connection_path_tsv, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    # reader_options are also needed to specify the separator (otherwise, comma will be used as the default separator)
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(  # noqa: PTH118
                test_folder_connection_path_tsv, "test.tsv"
            ),
            reader_options={"header": True, "sep": "\t"},
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


@pytest.mark.skipif(
    not pyarrow.pyarrow,
    reason='Could not import "pyarrow"',
)
def test_get_batch_empty_splitter_parquet(
    test_folder_connection_path_parquet, basic_spark_df_execution_engine
):
    # Note: reader method and reader_options are not needed, because
    # SparkDFExecutionEngine automatically determines the file type as well as the schema of the Parquet file.
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(  # noqa: PTH118
                test_folder_connection_path_parquet, "test.parquet"
            ),
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_runtime(
    test_sparkdf, basic_spark_df_execution_engine
):
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, splitter_method="_split_on_whole_table"
        )
    ).dataframe
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10


def test_get_batch_with_split_on_whole_table_filesystem(
    test_folder_connection_path_csv, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(  # noqa: PTH118
                test_folder_connection_path_csv, "test.csv"
            ),
            splitter_method="_split_on_whole_table",
        )
    ).dataframe
    assert test_sparkdf.count() == 6
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_s3(
    spark_session, basic_spark_df_execution_engine
):
    # noinspection PyUnusedLocal
    def mocked_get_reader_function(*args, **kwargs):
        # noinspection PyUnusedLocal,PyShadowingNames
        def mocked_reader_function(*args, **kwargs):
            pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
            df = spark_session.createDataFrame(
                [
                    tuple(
                        None if isinstance(x, (float, int)) and np.isnan(x) else x
                        for x in record.tolist()
                    )
                    for record in pd_df.to_records(index=False)
                ],
                pd_df.columns.tolist(),
            )
            return df

        return mocked_reader_function

    spark_engine = basic_spark_df_execution_engine
    spark_engine._get_reader_fn = mocked_get_reader_function

    test_sparkdf = spark_engine.get_batch_data(
        S3BatchSpec(
            path="s3://bucket/test/test.csv",
            reader_method="csv",
            reader_options={"header": True},
            splitter_method="_split_on_whole_table",
        )
    ).dataframe
    assert test_sparkdf.count() == 4
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_azure(
    spark_session, basic_spark_df_execution_engine
):
    # noinspection PyUnusedLocal
    def mocked_get_reader_function(*args, **kwargs):
        # noinspection PyUnusedLocal,PyShadowingNames
        def mocked_reader_function(*args, **kwargs):
            pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
            df = spark_session.createDataFrame(
                [
                    tuple(
                        None if isinstance(x, (float, int)) and np.isnan(x) else x
                        for x in record.tolist()
                    )
                    for record in pd_df.to_records(index=False)
                ],
                pd_df.columns.tolist(),
            )
            return df

        return mocked_reader_function

    spark_engine = basic_spark_df_execution_engine
    spark_engine._get_reader_fn = mocked_get_reader_function

    test_sparkdf = spark_engine.get_batch_data(
        AzureBatchSpec(
            path="wasbs://test_container@test_account.blob.core.windows.net/test_dir/test_file.csv",
            reader_method="csv",
            reader_options={"header": True},
            splitter_method="_split_on_whole_table",
        )
    ).dataframe
    assert test_sparkdf.count() == 4
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_gcs(
    spark_session, basic_spark_df_execution_engine
):
    # noinspection PyUnusedLocal
    def mocked_get_reader_function(*args, **kwargs):
        # noinspection PyUnusedLocal,PyShadowingNames
        def mocked_reader_function(*args, **kwargs):
            pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
            df = spark_session.createDataFrame(
                [
                    tuple(
                        None if isinstance(x, (float, int)) and np.isnan(x) else x
                        for x in record.tolist()
                    )
                    for record in pd_df.to_records(index=False)
                ],
                pd_df.columns.tolist(),
            )
            return df

        return mocked_reader_function

    spark_engine = basic_spark_df_execution_engine
    spark_engine._get_reader_fn = mocked_get_reader_function

    test_sparkdf = spark_engine.get_batch_data(
        GCSBatchSpec(
            path="gcs://bucket/test/test.csv",
            reader_method="csv",
            reader_options={"header": True},
            splitter_method="_split_on_whole_table",
        )
    ).dataframe
    assert test_sparkdf.count() == 4
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_column_value(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "batch_id",
                "batch_identifiers": {"batch_id": 2},
            },
        )
    ).dataframe
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10
    collected = split_df.collect()
    for val in collected:
        assert val.batch_id == 2

    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "date",
                "batch_identifiers": {"date": datetime.date(2020, 1, 30)},
            },
        )
    ).dataframe
    assert split_df.count() == 3
    assert len(split_df.columns) == 10


def test_get_batch_with_split_on_converted_datetime(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_converted_datetime",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": "2020-01-03"},
            },
        )
    ).dataframe
    assert split_df.count() == 2
    assert len(split_df.columns) == 10


def test_get_batch_with_split_on_divided_integer(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    ).dataframe
    assert split_df.count() == 10
    assert len(split_df.columns) == 10
    max_result = split_df.select([F.max("id")])
    assert max_result.collect()[0]["max(id)"] == 59
    min_result = split_df.select([F.min("id")])
    assert min_result.collect()[0]["min(id)"] == 50


def test_get_batch_with_split_on_mod_integer(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_mod_integer",
            splitter_kwargs={
                "column_name": "id",
                "mod": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    ).dataframe

    assert split_df.count() == 12
    assert len(split_df.columns) == 10
    max_result = split_df.select([F.max("id")])
    assert max_result.collect()[0]["max(id)"] == 115
    min_result = split_df.select([F.min("id")])
    assert min_result.collect()[0]["min(id)"] == 5


def test_get_batch_with_split_on_multi_column_values(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_multi_column_values",
            splitter_kwargs={
                "column_names": ["y", "m", "d"],
                "batch_identifiers": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                },
            },
        )
    ).dataframe
    assert split_df.count() == 4
    assert len(split_df.columns) == 10
    collected = split_df.collect()
    for val in collected:
        assert val.date == datetime.date(2020, 1, 5)

    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        split_df = basic_spark_df_execution_engine.get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                splitter_method="_split_on_multi_column_values",
                splitter_kwargs={
                    "column_names": ["I", "dont", "exist"],
                    "batch_identifiers": {
                        "y": 2020,
                        "m": 1,
                        "d": 5,
                    },
                },
            )
        ).dataframe


def test_get_batch_with_split_on_hashed_column_incorrect_hash_function_name(
    test_sparkdf,
    basic_spark_df_execution_engine,
):
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        _ = basic_spark_df_execution_engine.get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                splitter_method="_split_on_hashed_column",
                splitter_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "hash_function_name": "I_wont_work",
                    "batch_identifiers": {
                        "hash_value": "a",
                    },
                },
            )
        ).dataframe


def test_get_batch_with_split_on_hashed_column(
    test_sparkdf, basic_spark_df_execution_engine
):
    split_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_hashed_column",
            splitter_kwargs={
                "column_name": "favorite_color",
                "hash_digits": 1,
                "hash_function_name": "sha256",
                "batch_identifiers": {
                    "hash_value": "a",
                },
            },
        )
    ).dataframe
    assert split_df.count() == 8
    assert len(split_df.columns) == 10
