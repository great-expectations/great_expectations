import datetime
import logging
import os
import random

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import Batch
from great_expectations.datasource.types.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics import (
    ColumnMean,
    ColumnStandardDeviation,
    ColumnValuesInSet,
    ColumnValuesUnique,
    ColumnValuesZScore,
)
from great_expectations.validator.validation_graph import MetricConfiguration

try:
    pyspark = pytest.importorskip("pyspark")
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, StringType
except ImportError:
    pyspark = None
    F = None
    IntegerType = None
    StringType = None


def _build_spark_engine(spark_session, df):
    df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    engine = SparkDFExecutionEngine(batch_data_dict={"temp_id": df})
    return engine


@pytest.fixture
def test_sparkdf(spark_session):
    def generate_ascending_list_of_datetimes(
        k, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ):
        start_time = datetime.datetime(
            start_date.year, start_date.month, start_date.day
        )
        seconds_between_dates = (end_date - start_date).total_seconds()
        datetime_list = [
            start_time
            + datetime.timedelta(seconds=random.randrange(seconds_between_dates))
            for i in range(k)
        ]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)
    timestamp_list = generate_ascending_list_of_datetimes(
        k, end_date=datetime.date(2020, 1, 31)
    )
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    batch_ids = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()
    session_ids = [random.randint(2, 60) for i in range(k)]
    session_ids = [i - random.randint(0, 2) for i in session_ids]
    session_ids.sort()

    spark_df = spark_session.createDataFrame(
        data=pd.DataFrame(
            {
                "id": range(k),
                "batch_id": batch_ids,
                "date": date_list,
                "y": [d.year for d in date_list],
                "m": [d.month for d in date_list],
                "d": [d.day for d in date_list],
                "timestamp": timestamp_list,
                "session_ids": session_ids,
                "event_type": [
                    random.choice(["start", "stop", "continue"]) for i in range(k)
                ],
                "favorite_color": [
                    "#"
                    + "".join(
                        [random.choice(list("0123456789ABCDEF")) for j in range(6)]
                    )
                    for i in range(k)
                ],
            }
        )
    )
    spark_df = spark_df.withColumn(
        "timestamp", F.col("timestamp").cast(IntegerType()).cast(StringType())
    )
    return spark_df


def test_reader_fn(spark_session):
    engine = SparkDFExecutionEngine()
    # Testing that can recognize basic csv file
    fn = engine._get_reader_fn(reader=spark_session.read, path="myfile.csv")
    assert "<bound method DataFrameReader.csv" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader=spark_session.read, reader_method="csv")
    assert "<bound method DataFrameReader.csv" in str(fn_new)


def test_get_compute_domain_with_no_domain_kwargs(spark_session):
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
    engine = SparkDFExecutionEngine()
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type=MetricDomainTypes.TABLE
    )
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}
    assert data.schema == df.schema
    assert data.collect() == df.collect()


def test_get_compute_domain_with_column_domain(spark_session):
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
    engine = SparkDFExecutionEngine()
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}
    assert data.schema == df.schema
    assert data.collect() == df.collect()


def test_get_compute_domain_with_row_condition(spark_session):
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
    expected_df = df.filter(F.col("b") > 2)

    engine = SparkDFExecutionEngine()
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 2", "condition_parser": "spark"},
        domain_type=MetricDomainTypes.TABLE,
    )
    # Ensuring data has been properly queried
    assert data.schema == expected_df.schema
    assert data.collect() == expected_df.collect()

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition(spark_session):
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
    expected_df = df.filter(F.col("b") > 24)

    engine = SparkDFExecutionEngine()
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 24", "condition_parser": "spark"},
        domain_type=MetricDomainTypes.TABLE,
    )
    # Ensuring data has been properly queried
    assert data.schema == expected_df.schema
    assert data.collect() == expected_df.collect()

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys()
    assert accessor_kwargs == {}


def test_basic_setup(spark_session):
    pd_df = pd.DataFrame({"x": range(10)})
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
    batch_data = SparkDFExecutionEngine().get_batch_data(
        batch_spec=RuntimeDataBatchSpec(
            batch_data=df,
            data_asset_name="DATA_ASSET",
        )
    )
    assert batch_data is not None


def test_get_batch_data(test_sparkdf):
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_sparkdf, data_asset_name="DATA_ASSET")
    )
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10


def test_get_batch_empty_splitter(test_folder_connection_path_csv):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
            reader_options={"header": True},
            splitter_method=None,
        )
    )
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_empty_splitter_tsv(test_folder_connection_path_tsv):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    # reader_options are also needed to specify the separator (otherwise, comma will be used as the default separator)
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_tsv, "test.tsv"),
            reader_options={"header": True, "sep": "\t"},
            splitter_method=None,
        )
    )
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_empty_splitter_parquet(test_folder_connection_path_parquet):
    # Note: reader method and reader_options are not needed, because
    # SparkDFExecutionEngine automatically determines the file type as well as the schema of the Parquet file.
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_parquet, "test.parquet"),
            splitter_method=None,
        )
    )
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_filesystem(
    test_folder_connection_path_csv,
):
    # reader_method not configured because spark will configure own reader by default
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_sparkdf.count() == 6
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_s3(spark_session):
    def mocked_get_reader_function(*args, **kwargs):
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

    spark_engine = SparkDFExecutionEngine()
    spark_engine._get_reader_fn = mocked_get_reader_function

    test_sparkdf = spark_engine.get_batch_data(
        S3BatchSpec(
            s3="s3://bucket/test/test.csv",
            reader_method="csv",
            reader_options={"header": True},
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_sparkdf.count() == 4
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table(test_sparkdf):
    test_sparkdf = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, splitter_method="_split_on_whole_table"
        )
    )
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10


def test_get_batch_with_split_on_column_value(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "batch_id",
                "partition_definition": {"batch_id": 2},
            },
        )
    )
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10
    collected = split_df.collect()
    for val in collected:
        assert val.batch_id == 2

    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "date",
                "partition_definition": {"date": datetime.date(2020, 1, 30)},
            },
        )
    )
    assert split_df.count() == 3
    assert len(split_df.columns) == 10


def test_get_batch_with_split_on_converted_datetime(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_converted_datetime",
            splitter_kwargs={
                "column_name": "timestamp",
                "partition_definition": {"timestamp": "2020-01-03"},
            },
        )
    )
    assert split_df.count() == 2
    assert len(split_df.columns) == 10


def test_get_batch_with_split_on_divided_integer(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "partition_definition": {"id": 5},
            },
        )
    )
    assert split_df.count() == 10
    assert len(split_df.columns) == 10
    max_result = split_df.select([F.max("id")])
    assert max_result.collect()[0]["max(id)"] == 59
    min_result = split_df.select([F.min("id")])
    assert min_result.collect()[0]["min(id)"] == 50


def test_get_batch_with_split_on_mod_integer(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_mod_integer",
            splitter_kwargs={
                "column_name": "id",
                "mod": 10,
                "partition_definition": {"id": 5},
            },
        )
    )

    assert split_df.count() == 12
    assert len(split_df.columns) == 10
    max_result = split_df.select([F.max("id")])
    assert max_result.collect()[0]["max(id)"] == 115
    min_result = split_df.select([F.min("id")])
    assert min_result.collect()[0]["min(id)"] == 5


def test_get_batch_with_split_on_multi_column_values(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_multi_column_values",
            splitter_kwargs={
                "column_names": ["y", "m", "d"],
                "partition_definition": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                },
            },
        )
    )
    assert split_df.count() == 4
    assert len(split_df.columns) == 10
    collected = split_df.collect()
    for val in collected:
        assert val.date == datetime.date(2020, 1, 5)

    with pytest.raises(ValueError):
        split_df = SparkDFExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                splitter_method="_split_on_multi_column_values",
                splitter_kwargs={
                    "column_names": ["I", "dont", "exist"],
                    "partition_definition": {
                        "y": 2020,
                        "m": 1,
                        "d": 5,
                    },
                },
            )
        )


def test_get_batch_with_split_on_hashed_column_incorrect_hash_function_name(
    test_sparkdf,
):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        split_df = SparkDFExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                splitter_method="_split_on_hashed_column",
                splitter_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "hash_function_name": "I_wont_work",
                    "partition_definition": {
                        "hash_value": "a",
                    },
                },
            )
        )


def test_get_batch_with_split_on_hashed_column(test_sparkdf):
    split_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_hashed_column",
            splitter_kwargs={
                "column_name": "favorite_color",
                "hash_digits": 1,
                "hash_function_name": "sha256",
                "partition_definition": {
                    "hash_value": "a",
                },
            },
        )
    )
    assert split_df.count() == 8
    assert len(split_df.columns) == 10


# ### Sampling methods ###
def test_get_batch_empty_sampler(test_sparkdf):
    sampled_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_sparkdf, sampling_method=None)
    )
    assert sampled_df.count() == 120
    assert len(sampled_df.columns) == 10


def test_sample_using_random(test_sparkdf):
    sampled_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, sampling_method="_sample_using_random"
        )
    )
    # The test dataframe contains 10 columns and 120 rows.
    assert len(sampled_df.columns) == 10
    assert 0 <= sampled_df.count() <= 120
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.1 (the equivalent of an
    # unfair coin with the 10% chance of coming up as "heads").  Hence, we should never get as much as 20% of the rows.
    assert sampled_df.count() < 25


def test_sample_using_mod(test_sparkdf):
    sampled_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    )
    assert sampled_df.count() == 24
    assert len(sampled_df.columns) == 10


def test_sample_using_a_list(test_sparkdf):
    sampled_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_a_list",
            sampling_kwargs={
                "column_name": "id",
                "value_list": [3, 5, 7, 11],
            },
        )
    )
    assert sampled_df.count() == 4
    assert len(sampled_df.columns) == 10


def test_sample_using_md5_wrong_hash_function_name(test_sparkdf):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        sampled_df = SparkDFExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                sampling_method="_sample_using_hash",
                sampling_kwargs={
                    "column_name": "date",
                    "hash_function_name": "I_wont_work",
                },
            )
        )


def test_sample_using_md5(test_sparkdf):
    sampled_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_hash",
            sampling_kwargs={
                "column_name": "date",
                "hash_function_name": "md5",
            },
        )
    )
    assert sampled_df.count() == 10
    assert len(sampled_df.columns) == 10

    collected = sampled_df.collect()
    for val in collected:
        assert val.date in [datetime.date(2020, 1, 15), datetime.date(2020, 1, 29)]


def test_split_on_multi_column_values_and_sample_using_random(test_sparkdf):
    returned_df = SparkDFExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            splitter_method="_split_on_multi_column_values",
            splitter_kwargs={
                "column_names": ["y", "m", "d"],
                "partition_definition": {
                    "y": 2020,
                    "m": 1,
                    "d": 5,
                },
            },
            sampling_method="_sample_using_random",
            sampling_kwargs={
                "p": 0.5,
            },
        )
    )

    # The test dataframe contains 10 columns and 120 rows.
    assert len(returned_df.columns) == 10
    # The number of returned rows corresponding to the value of "partition_definition" above is 4.
    assert 0 <= returned_df.count() <= 4
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.5 (the equivalent of a
    # fair coin with the 50% chance of coming up as "heads").  Hence, on average we should get 50% of the rows, which is
    # 2; however, for such a small sample (of 4 rows), the number of rows returned by an individual run can deviate from
    # this average.  Still, in the majority of trials, the number of rows should not be fewer than 2 or greater than 3.
    # The assertion in the next line, supporting this reasoning, is commented out to insure zero failures.  Developers
    # are encouraged to uncomment it, whenever the "_sample_using_random" feature is the main focus of a given effort.
    # assert 2 <= returned_df.count() <= 3

    for val in returned_df.collect():
        assert val.date == datetime.date(2020, 1, 5)


def test_add_column_row_condition(spark_session):
    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})
    domain_kwargs = {"column": "foo"}

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo) AND foo IS NOT NULL"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (None,), (2,), (3,), (4,), (5,), (6,)]

    # This time, our skip value *will* be nan
    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(df)
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    expected = [(1,), (2,), (3,), (3,), (np.nan,), (2,), (3,), (4,), (5,), (6,)]
    # since nan != nan by default
    assert np.allclose(res, expected, rtol=0, atol=0, equal_nan=True)


# Function to test for spark dataframe equality
def dataframes_equal(first_table, second_table):
    if first_table.schema != second_table.schema:
        return False
    if first_table.collect() != second_table.collect():
        return False
    return True


# Builds a Spark Execution Engine
def _build_spark_engine(df):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    batch = Batch(data=df)
    engine = SparkDFExecutionEngine(batch_data_dict={batch.id: batch.data})
    return engine


# Ensuring that, given aggregate metrics, they can be properly bundled together
def test_sparkdf_batch_aggregate_metrics(caplog, spark_session):
    import datetime

    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        )
    )
    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_1},
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_2},
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_3},
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_4},
    )
    start = datetime.datetime.now()
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    res = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    end = datetime.datetime.now()
    print(end - start)
    assert res[desired_metric_1.id] == 3
    assert res[desired_metric_2.id] == 1
    assert res[desired_metric_3.id] == 4
    assert res[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SparkDFExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


# Ensuring functionality of compute_domain when no domain kwargs are given
def test_get_compute_domain_with_no_domain_kwargs():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="table"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_column_pair():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="column_pair"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Accessor kwargs have been modified"

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="identity"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Compute domain kwargs should not be modified"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="multicolumn"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be empty"
    assert accessor_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"

    # Checking for identity
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="identity"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Compute domain kwargs should not change for identity domain"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type="column"
    )

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be empty"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# Using an unmeetable row condition to see if empty dataset will result in errors
def test_get_compute_domain_with_row_condition():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe
    expected_df = df.where("b > 2")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 2", "condition_parser": "spark"},
        domain_type="identity",
    )

    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe
    expected_df = df.where("b > 24")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "row_condition": "b > 24",
            "condition_parser": "spark",
        },
        domain_type="identity",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"

    # Ensuring errors for column and column_ pair domains are caught
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column",
        )
    with pytest.raises(GreatExpectationsError) as g:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column_pair",
        )


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Filtering expected data based on row condition
    expected_df = df.where("b == 2")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Obtaining data from computation
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="column",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "b"}, "Accessor kwargs have been modified"

    # Should react differently for domain type identity
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="identity",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df.select("b")
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_nonexistent_condition_parser():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
            domain_type=MetricDomainTypes.IDENTITY,
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.does_not_exist",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )

    # Ensuring a metric provider error is raised if metric does not exist
    with pytest.raises(MetricProviderError) as e:
        res = engine.resolve_metrics(
            metrics_to_resolve=(
                desired_metric_1,
                desired_metric_2,
                desired_metric_3,
                desired_metric_4,
            )
        )
        print(e)


# Making sure dataframe property is functional
def test_dataframe_property_given_loaded_batch():
    from pyspark.sql import SparkSession

    engine = SparkDFExecutionEngine()

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Ensuring Data not distorted
    assert engine.dataframe == df
