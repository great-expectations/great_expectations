import datetime
import logging
import os
import random

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.self_check.util import build_spark_engine
from great_expectations.validator.validation_graph import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric

try:
    pyspark = pytest.importorskip("pyspark")
    # noinspection PyPep8Naming
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, StringType
except ImportError:
    pyspark = None
    F = None
    IntegerType = None
    StringType = None


@pytest.fixture
def test_sparkdf(spark_session):
    def generate_ascending_list_of_datetimes(
        n, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ):
        start_time = datetime.datetime(
            start_date.year, start_date.month, start_date.day
        )
        seconds_between_dates = (end_date - start_date).total_seconds()
        # noinspection PyUnusedLocal
        datetime_list = [
            start_time
            + datetime.timedelta(seconds=random.randrange(int(seconds_between_dates)))
            for i in range(n)
        ]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)
    timestamp_list = generate_ascending_list_of_datetimes(
        n=k, end_date=datetime.date(2020, 1, 31)
    )
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    # noinspection PyUnusedLocal
    batch_ids = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()
    # noinspection PyUnusedLocal
    session_ids = [random.randint(2, 60) for i in range(k)]
    session_ids = [i - random.randint(0, 2) for i in session_ids]
    session_ids.sort()

    # noinspection PyUnusedLocal
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


def test_reader_fn(spark_session, basic_spark_df_execution_engine):
    engine = basic_spark_df_execution_engine
    # Testing that can recognize basic csv file
    fn = engine._get_reader_fn(reader=spark_session.read, path="myfile.csv")
    assert "<bound method DataFrameReader.csv" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader=spark_session.read, reader_method="csv")
    assert "<bound method DataFrameReader.csv" in str(fn_new)


def test_get_compute_domain_with_no_domain_kwargs(
    spark_session, basic_spark_df_execution_engine
):
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
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type=MetricDomainTypes.TABLE
    )
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}
    assert data.schema == df.schema
    assert data.collect() == df.collect()


def test_get_compute_domain_with_column_domain(
    spark_session, basic_spark_df_execution_engine
):
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
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}
    assert data.schema == df.schema
    assert data.collect() == df.collect()


def test_get_compute_domain_with_row_condition(
    spark_session, basic_spark_df_execution_engine
):
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

    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)

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
def test_get_compute_domain_with_unmeetable_row_condition(
    spark_session, basic_spark_df_execution_engine
):
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

    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)

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


def test_basic_setup(spark_session, basic_spark_df_execution_engine):
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
    batch_data = basic_spark_df_execution_engine.get_batch_data(
        batch_spec=RuntimeDataBatchSpec(
            batch_data=df,
            data_asset_name="DATA_ASSET",
        )
    ).dataframe
    assert batch_data is not None


def test_get_batch_data(test_sparkdf, basic_spark_df_execution_engine):
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_sparkdf, data_asset_name="DATA_ASSET")
    ).dataframe
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10


def test_get_batch_empty_splitter(
    test_folder_connection_path_csv, spark_session, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
            reader_options={"header": True},
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_empty_splitter_tsv(
    test_folder_connection_path_tsv, spark_session, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    # reader_options are needed to specify the fact that the first line of test file is the header
    # reader_options are also needed to specify the separator (otherwise, comma will be used as the default separator)
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_tsv, "test.tsv"),
            reader_options={"header": True, "sep": "\t"},
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_empty_splitter_parquet(
    test_folder_connection_path_parquet, spark_session, basic_spark_df_execution_engine
):
    # Note: reader method and reader_options are not needed, because
    # SparkDFExecutionEngine automatically determines the file type as well as the schema of the Parquet file.
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_parquet, "test.parquet"),
            splitter_method=None,
        )
    ).dataframe
    assert test_sparkdf.count() == 5
    assert len(test_sparkdf.columns) == 2


def test_get_batch_with_split_on_whole_table_filesystem(
    test_folder_connection_path_csv, spark_session, basic_spark_df_execution_engine
):
    # reader_method not configured because spark will configure own reader by default
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
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


def test_get_batch_with_split_on_whole_table(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    test_sparkdf = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, splitter_method="_split_on_whole_table"
        )
    ).dataframe
    assert test_sparkdf.count() == 120
    assert len(test_sparkdf.columns) == 10


def test_get_batch_with_split_on_column_value(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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
    spark_session,
    basic_spark_df_execution_engine,
):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        split_df = basic_spark_df_execution_engine.get_batch_data(
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
    test_sparkdf, spark_session, basic_spark_df_execution_engine
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


# ### Sampling methods ###
def test_get_batch_empty_sampler(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_sparkdf, sampling_method=None)
    ).dataframe
    assert sampled_df.count() == 120
    assert len(sampled_df.columns) == 10


def test_sample_using_random(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf, sampling_method="_sample_using_random"
        )
    ).dataframe
    # The test dataframe contains 10 columns and 120 rows.
    assert len(sampled_df.columns) == 10
    assert 0 <= sampled_df.count() <= 120
    # The sampling probability "p" used in "SparkDFExecutionEngine._sample_using_random()" is 0.1 (the equivalent of an
    # unfair coin with the 10% chance of coming up as "heads").  Hence, we should never get as much as 20% of the rows.
    assert sampled_df.count() < 25


def test_sample_using_mod(test_sparkdf, spark_session, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    ).dataframe
    assert sampled_df.count() == 24
    assert len(sampled_df.columns) == 10


def test_sample_using_a_list(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_a_list",
            sampling_kwargs={
                "column_name": "id",
                "value_list": [3, 5, 7, 11],
            },
        )
    ).dataframe
    assert sampled_df.count() == 4
    assert len(sampled_df.columns) == 10


def test_sample_using_md5_wrong_hash_function_name(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        sampled_df = basic_spark_df_execution_engine.get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_sparkdf,
                sampling_method="_sample_using_hash",
                sampling_kwargs={
                    "column_name": "date",
                    "hash_function_name": "I_wont_work",
                },
            )
        ).dataframe


def test_sample_using_md5(test_sparkdf, spark_session, basic_spark_df_execution_engine):
    sampled_df = basic_spark_df_execution_engine.get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_sparkdf,
            sampling_method="_sample_using_hash",
            sampling_kwargs={
                "column_name": "date",
                "hash_function_name": "md5",
            },
        )
    ).dataframe
    assert sampled_df.count() == 10
    assert len(sampled_df.columns) == 10

    collected = sampled_df.collect()
    for val in collected:
        assert val.date in [datetime.date(2020, 1, 15), datetime.date(2020, 1, 29)]


def test_split_on_multi_column_values_and_sample_using_random(
    test_sparkdf, spark_session, basic_spark_df_execution_engine
):
    returned_df = basic_spark_df_execution_engine.get_batch_data(
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
            sampling_method="_sample_using_random",
            sampling_kwargs={
                "p": 0.5,
            },
        )
    ).dataframe

    # The test dataframe contains 10 columns and 120 rows.
    assert len(returned_df.columns) == 10
    # The number of returned rows corresponding to the value of "batch_identifiers" above is 4.
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


def test_add_column_row_condition(spark_session, basic_spark_df_execution_engine):
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
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
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
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)

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


# Ensuring that, given aggregate metrics, they can be properly bundled together
def test_sparkdf_batch_aggregate_metrics(caplog, spark_session):
    import datetime

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]},
        ),
        batch_id="1234",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)

    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_1,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_2,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_3,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_4,
            "table.columns": table_columns_metric,
        },
    )
    start = datetime.datetime.now()
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)
    end = datetime.datetime.now()
    print(end - start)
    assert metrics[desired_metric_1.id] == 3
    assert metrics[desired_metric_2.id] == 1
    assert metrics[desired_metric_3.id] == 4
    assert metrics[desired_metric_4.id] == 4

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
def test_get_compute_domain_with_no_domain_kwargs_alt(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

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
def test_get_compute_domain_with_column_pair(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

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
def test_get_compute_domain_with_multicolumn(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

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
def test_get_compute_domain_with_column_domain_alt(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

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
def test_get_compute_domain_with_row_condition_alt(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
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
def test_get_compute_domain_with_unmeetable_row_condition_alt(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe
    expected_df = df.where("b > 24")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 24", "condition_parser": "spark"},
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
    with pytest.raises(GreatExpectationsError):
        # noinspection PyUnusedLocal
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column",
        )
    with pytest.raises(GreatExpectationsError) as g:
        # noinspection PyUnusedLocal
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column_pair",
        )


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
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


def test_get_compute_domain_with_nonexistent_condition_parser(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(GreatExpectationsError):
        # noinspection PyUnusedLocal
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
            domain_type=MetricDomainTypes.IDENTITY,
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]},
        ),
        batch_id="1234",
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
        # noinspection PyUnusedLocal
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
def test_dataframe_property_given_loaded_batch(
    spark_session, basic_spark_df_execution_engine
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 5, 22, 3, 5, 10]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

    # Ensuring Data not distorted
    assert engine.dataframe == df
