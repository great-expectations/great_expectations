import datetime
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.batch_spec import PathBatchSpec, RuntimeDataBatchSpec
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
)
from great_expectations.self_check.util import build_spark_engine
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric
from tests.test_utils import create_files_in_directory


def test_reader_fn(spark_session, basic_spark_df_execution_engine):
    engine = basic_spark_df_execution_engine
    # Testing that can recognize basic csv file
    fn = engine._get_reader_fn(reader=spark_session.read, path="myfile.csv")
    assert "<bound method DataFrameReader.csv" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader=spark_session.read, reader_method="csv")
    assert "<bound method DataFrameReader.csv" in str(fn_new)


@pytest.mark.integration
def test_reader_fn_parameters(
    spark_session, basic_spark_df_execution_engine, tmp_path_factory
):
    base_directory = str(tmp_path_factory.mktemp("test_csv"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test-A.csv",
        ],
    )
    test_df_small_csv_path = base_directory + "/test-A.csv"
    engine = basic_spark_df_execution_engine
    fn = engine._get_reader_fn(reader=spark_session.read, path=test_df_small_csv_path)
    assert "<bound method DataFrameReader.csv" in str(fn)

    test_sparkdf_with_no_header_param = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(path=test_df_small_csv_path, data_asset_name="DATA_ASSET")
    ).dataframe
    assert test_sparkdf_with_no_header_param.head() == pyspark.Row(_c0="x", _c1="y")

    test_sparkdf_with_header_param = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(
            path=test_df_small_csv_path,
            data_asset_name="DATA_ASSET",
            reader_options={"header": True},
        )
    ).dataframe
    assert test_sparkdf_with_header_param.head() == pyspark.Row(x="1", y="2")

    test_sparkdf_with_no_header_param = basic_spark_df_execution_engine.get_batch_data(
        PathBatchSpec(path=test_df_small_csv_path, data_asset_name="DATA_ASSET")
    ).dataframe
    assert test_sparkdf_with_no_header_param.head() == pyspark.Row(_c0="x", _c1="y")

    # defining schema
    schema: pyspark.types.StructType = pyspark.types.StructType(
        [
            pyspark.types.StructField("x", pyspark.types.IntegerType(), True),
            pyspark.types.StructField("y", pyspark.types.IntegerType(), True),
        ]
    )
    schema_dict: dict = schema

    test_sparkdf_with_header_param_and_schema = (
        basic_spark_df_execution_engine.get_batch_data(
            PathBatchSpec(
                path=test_df_small_csv_path,
                data_asset_name="DATA_ASSET",
                reader_options={"header": True, "schema": schema_dict},
            )
        ).dataframe
    )
    assert test_sparkdf_with_header_param_and_schema.head() == pyspark.Row(x=1, y=2)
    assert test_sparkdf_with_header_param_and_schema.schema == schema_dict


def test_get_domain_records_with_column_domain(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("b")<5',
            "condition_parser": "great_expectations__experimental__",
        }
    )

    expected_column_pd_df = pd_df.iloc[:3]
    expected_column_df = spark_df_from_pandas_df(spark_session, expected_column_pd_df)

    assert dataframes_equal(
        data, expected_column_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_column_domain_and_filter_conditions(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("b")<5',
            "condition_parser": "great_expectations__experimental__",
            "filter_conditions": [
                RowCondition(
                    condition="b IS NOT NULL",
                    condition_type=RowConditionParserType.SPARK_SQL,
                )
            ],
        }
    )

    expected_column_pd_df = pd_df.iloc[:3]
    expected_column_df = spark_df_from_pandas_df(spark_session, expected_column_pd_df)

    assert dataframes_equal(
        data, expected_column_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_different_column_domain_and_filter_conditions(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("a")<2',
            "condition_parser": "great_expectations__experimental__",
            "filter_conditions": [
                RowCondition(
                    condition="b IS NOT NULL",
                    condition_type=RowConditionParserType.SPARK_SQL,
                )
            ],
        }
    )

    expected_column_pd_df = pd_df.iloc[:1]
    expected_column_df = spark_df_from_pandas_df(spark_session, expected_column_pd_df)

    assert dataframes_equal(
        data, expected_column_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_different_column_domain_and_multiple_filter_conditions(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("a")<10',
            "condition_parser": "great_expectations__experimental__",
            "filter_conditions": [
                RowCondition(
                    condition="b IS NOT NULL",
                    condition_type=RowConditionParserType.SPARK_SQL,
                ),
                RowCondition(
                    condition="NOT isnan(b)",
                    condition_type=RowConditionParserType.SPARK_SQL,
                ),
            ],
        }
    )

    expected_column_pd_df = pd_df.iloc[:4]
    expected_column_df = spark_df_from_pandas_df(spark_session, expected_column_pd_df)

    assert dataframes_equal(
        data, expected_column_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_column_pair_domain(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "both_values_are_missing",
        }
    )

    expected_column_pair_pd_df = pd.DataFrame(
        {"a": [2, 3, 4, 6], "b": [3.0, 4.0, 5.0, 6.0], "c": [2.0, 3.0, 4.0, None]}
    )
    expected_column_pair_df = spark_df_from_pandas_df(
        spark_session, expected_column_pair_pd_df
    )

    assert dataframes_equal(
        data, expected_column_pair_df
    ), "Data does not match after getting full access compute domain"

    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "either_value_is_missing",
        }
    )
    for column_name in data.columns:
        data = data.withColumn(
            column_name, data[column_name].cast(pyspark.types.LongType())
        )

    expected_column_pair_pd_df = pd.DataFrame(
        {"a": [2, 3, 4], "b": [3, 4, 5], "c": [2, 3, 4]}
    )
    expected_column_pair_df = spark_df_from_pandas_df(
        spark_session, expected_column_pair_pd_df
    )

    assert dataframes_equal(
        data, expected_column_pair_df
    ), "Data does not match after getting full access compute domain"

    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": 'col("a")<6',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "neither",
        }
    )

    expected_column_pair_pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [2.0, 3.0, 4.0, 5.0, None],
            "c": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )
    expected_column_pair_df = spark_df_from_pandas_df(
        spark_session, expected_column_pair_pd_df
    )

    assert dataframes_equal(
        data, expected_column_pair_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_multicolumn_domain(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["a", "c"],
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "all_values_are_missing",
        }
    )
    for column_name in data.columns:
        data = data.withColumn(
            column_name, data[column_name].cast(pyspark.types.LongType())
        )

    expected_multicolumn_pd_df = pd.DataFrame(
        {"a": [2, 3, 4, 5], "b": [3, 4, 5, 7], "c": [2, 3, 4, 6]}, index=[0, 1, 2, 4]
    )
    expected_multicolumn_df = spark_df_from_pandas_df(
        spark_session, expected_multicolumn_pd_df
    )

    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=expected_multicolumn_df)

    assert dataframes_equal(
        data, expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"

    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "row_condition": 'col("a")<5',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "any_value_is_missing",
        }
    )
    for column_name in data.columns:
        data = data.withColumn(
            column_name, data[column_name].cast(pyspark.types.LongType())
        )

    expected_multicolumn_pd_df = pd.DataFrame(
        {"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [1, 2, 3, 4]}, index=[0, 1, 2, 3]
    )

    expected_multicolumn_df = spark_df_from_pandas_df(
        spark_session, expected_multicolumn_pd_df
    )

    assert dataframes_equal(
        data, expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"

    pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    df = spark_df_from_pandas_df(spark_session, pd_df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="1234", batch_data=df)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "ignore_row_if": "never",
        }
    )

    expected_multicolumn_pd_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        },
        index=[0, 1, 2, 3, 4, 5],
    )

    expected_multicolumn_df = spark_df_from_pandas_df(
        spark_session, expected_multicolumn_pd_df
    )

    assert dataframes_equal(
        data, expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"


def test_get_compute_domain_with_no_domain_kwargs(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    df = spark_df_from_pandas_df(spark_session, pd_df)
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
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    df = spark_df_from_pandas_df(spark_session, pd_df)
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
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    df = spark_df_from_pandas_df(spark_session, pd_df)
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
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    df = spark_df_from_pandas_df(spark_session, pd_df)
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


def test_basic_setup(
    spark_session, basic_spark_df_execution_engine, spark_df_from_pandas_df
):
    pd_df = pd.DataFrame({"x": range(10)})
    df = spark_df_from_pandas_df(spark_session, pd_df)
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


def test_split_on_multi_column_values_and_sample_using_random(
    test_sparkdf, basic_spark_df_execution_engine
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
    assert new_domain_kwargs["filter_conditions"] == [
        RowCondition(
            condition="foo IS NOT NULL", condition_type=RowConditionParserType.SPARK_SQL
        )
    ]
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=True
    )
    assert new_domain_kwargs["filter_conditions"] == [
        RowCondition(
            condition="foo IS NOT NULL", condition_type=RowConditionParserType.SPARK_SQL
        ),
        RowCondition(
            condition="NOT isnan(foo)", condition_type=RowConditionParserType.SPARK_SQL
        ),
    ]
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["filter_conditions"] == [
        RowCondition(
            condition="NOT isnan(foo)", condition_type=RowConditionParserType.SPARK_SQL
        )
    ]
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
    assert new_domain_kwargs["filter_conditions"] == [
        RowCondition(
            condition="NOT isnan(foo)", condition_type=RowConditionParserType.SPARK_SQL
        )
    ]
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["filter_conditions"] == [
        RowCondition(
            condition="foo IS NOT NULL", condition_type=RowConditionParserType.SPARK_SQL
        ),
    ]
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

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

    metrics.update(results)

    desired_aggregate_fn_metric_1 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_1.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_2 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_2.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_3 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_3.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_4 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_4.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_aggregate_fn_metric_1,
            desired_aggregate_fn_metric_2,
            desired_aggregate_fn_metric_3,
            desired_aggregate_fn_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_1.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_1,
        "table.columns": table_columns_metric,
    }
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_2.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_2,
        "table.columns": table_columns_metric,
    }
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_3.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_3,
        "table.columns": table_columns_metric,
    }
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_4,
        "table.columns": table_columns_metric,
    }
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
def test_get_compute_domain_with_no_domain_kwargs_alt(spark_session):
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
def test_get_compute_domain_with_column_pair(spark_session):
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


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]},
        ),
        batch_id="1234",
    )
    df = engine.dataframe

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_list": ["a", "b", "c"]}, domain_type="multicolumn"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be empty"
    assert accessor_kwargs == {
        "column_list": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain_alt(spark_session):
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
def test_get_domain_records_with_row_condition_alt(spark_session):
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

    data = engine.get_domain_records(
        domain_kwargs={
            "row_condition": "b > 2",
            "condition_parser": "spark",
        }
    )

    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"


# What happens when we filter such that no value meets the condition?
def test_get_domain_records_with_unmeetable_row_condition_alt(spark_session):
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

    data = engine.get_domain_records(
        domain_kwargs={
            "row_condition": "b > 24",
            "condition_parser": "spark",
        }
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring errors for column and column_ pair domains are caught
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        # noinspection PyUnusedLocal
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column",
        )
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        # noinspection PyUnusedLocal
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "spark",
            },
            domain_type="column_pair",
        )


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser(spark_session):
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

    # Should react same for get_domain_records()
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        }
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"


def test_get_compute_domain_with_nonexistent_condition_parser(spark_session):
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
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        # noinspection PyUnusedLocal
        engine.get_domain_records(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric(spark_session):
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
        metric_value_kwargs=None,
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.does_not_exist",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )

    # Ensuring a metric provider error is raised if metric does not exist
    with pytest.raises(gx_exceptions.MetricProviderError) as e:
        # noinspection PyUnusedLocal
        engine.resolve_metrics(
            metrics_to_resolve=(
                desired_metric_1,
                desired_metric_2,
                desired_metric_3,
                desired_metric_4,
            )
        )
        print(e)


def test_resolve_metric_bundle_with_compute_domain_kwargs_json_serialization(
    spark_session,
):
    """
    Insures that even when "compute_domain_kwargs" has multiple keys, it will be JSON-serialized for "IDDict.to_id()".
    """
    engine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "names": [
                    "Ada Lovelace",
                    "Alan Kay",
                    "Donald Knuth",
                    "Edsger Dijkstra",
                    "Guido van Rossum",
                    "John McCarthy",
                    "Marvin Minsky",
                    "Ray Ozzie",
                ]
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
            "batch_id": "my_id",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    try:
        results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))
    except gx_exceptions.MetricProviderError as e:
        assert False, str(e)

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.max",
        metric_domain_kwargs={
            "batch_id": "my_id",
        },
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }

    try:
        results = engine.resolve_metrics(
            metrics_to_resolve=(desired_metric,), metrics=results
        )
        assert results == {desired_metric.id: 16}
    except gx_exceptions.MetricProviderError as e:
        assert False, str(e)


# Making sure dataframe property is functional
def test_dataframe_property_given_loaded_batch(spark_session):
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


@pytest.mark.integration
def test_schema_properly_added(spark_session):
    schema: pyspark.types.StructType = pyspark.types.StructType(
        [
            pyspark.types.StructField("a", pyspark.types.IntegerType(), True),
        ]
    )
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 5, 22, 3, 5, 10]},
        ),
        batch_id="1234",
        schema=schema,
    )
    df = engine.dataframe
    assert df.schema == schema
