import datetime
import os
import random
from typing import List

import boto3
import pandas as pd
import pytest
from moto import mock_s3

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import Batch
from great_expectations.datasource.data_connector import (
    ConfiguredAssetS3DataConnector,
    InferredAssetS3DataConnector,
)
from great_expectations.datasource.types.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.validator.validation_graph import MetricConfiguration


def test_reader_fn():
    engine = PandasExecutionEngine()

    # Testing that can recognize basic excel file
    fn = engine._get_reader_fn(path="myfile.xlsx")
    assert "<function read_excel" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader_method="read_csv")
    assert "<function" in str(fn_new)


def test_get_compute_domain_with_no_domain_kwargs():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="identity"
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type=MetricDomainTypes.TABLE
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_pair_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [1, 2, 3, 4]})
    expected_identity = df.drop(columns=["c"])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="column_pair"
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="identity"
    )

    assert data.equals(
        expected_identity
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_multicolumn_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 2, 3], "d": [2, 7, 9, 2]}
    )
    expected_identity = df.drop(columns=["d"])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="multicolumn"
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="identity"
    )
    assert data.equals(
        expected_identity
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_identity = df.drop(columns=["b"])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"

    # Doing this using identity domain should yield different results
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.IDENTITY
    )

    assert data.equals(
        expected_identity
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {"column": "a"}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df["b"] > 2].reset_index()

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 2", "condition_parser": "pandas"},
        domain_type="table",
    )
    # Ensuring data has been properly queried
    assert data["b"].equals(
        expected_df["b"]
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df["b"] > 24].reset_index()

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "row_condition": "b > 24",
            "condition_parser": "pandas",
        },
        domain_type="identity",
    )
    # Ensuring data has been properly queried
    assert data["b"].equals(
        expected_df["b"]
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Just checking that the Pandas Execution Engine can perform these in sequence
def test_resolve_metric_bundle():
    df = pd.DataFrame({"a": [1, 2, 3, None]})

    # Building engine and configurations in attempt to resolve metrics
    engine = PandasExecutionEngine(batch_data_dict={"made-up-id": df})
    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)
    metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)

    # Ensuring metrics have been properly resolved
    assert (
        metrics[("column.mean", "column=a", ())] == 2.0
    ), "mean metric not properly computed"
    assert metrics[("column.standard_deviation", "column=a", ())] == 1.0, (
        "standard deviation " "metric not properly computed"
    )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric():
    df = pd.DataFrame({"a": [1, 2, 3, None]})

    # Building engine and configurations in attempt to resolve metrics
    engine = PandasExecutionEngine(batch_data_dict={"made_up_id": df})
    mean = MetricConfiguration(
        metric_name="column.i_don't_exist",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.nonexistent",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)

    with pytest.raises(MetricProviderError) as e:
        metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)


# Making sure dataframe property is functional
def test_dataframe_property_given_loaded_batch():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Ensuring Data not distorted
    assert engine.dataframe.equals(df)


def test_get_batch_data(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
        )
    )
    assert split_df.shape == (120, 10)

    # No dataset passed to RuntimeDataBatchSpec
    with pytest.raises(ge_exceptions.InvalidBatchSpecError):
        PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec())


def test_get_batch_with_split_on_whole_table(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df, splitter_method="_split_on_whole_table"
        )
    )
    assert split_df.shape == (120, 10)


def test_get_batch_with_split_on_whole_table_filesystem(
    test_folder_connection_path_csv,
):
    test_df = PandasExecutionEngine().get_batch_data(
        PathBatchSpec(
            path=os.path.join(test_folder_connection_path_csv, "test.csv"),
            reader_method="read_csv",
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_df.shape == (5, 2)


@mock_s3
def test_get_batch_with_split_on_whole_table_s3_with_configured_asset_s3_data_connector():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )
    path = "path/A-100.csv"
    full_path = f"s3a://{os.path.join(bucket, path)}"

    my_data_connector = ConfiguredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        bucket=bucket,
        prefix="",
        assets={"alpha": {}},
    )

    test_df = PandasExecutionEngine().get_batch_data(
        batch_spec=S3BatchSpec(
            s3=full_path,
            reader_method="read_csv",
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_df.shape == (2, 2)


@mock_s3
def test_get_batch_with_split_on_whole_table_s3():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    path = "path/A-100.csv"
    full_path = f"s3a://{os.path.join(bucket, path)}"
    test_df = PandasExecutionEngine().get_batch_data(
        batch_spec=S3BatchSpec(
            s3=full_path,
            reader_method="read_csv",
            splitter_method="_split_on_whole_table",
        )
    )
    assert test_df.shape == (2, 2)

    # if S3 was not configured
    execution_engine_no_s3 = PandasExecutionEngine()
    execution_engine_no_s3._s3 = None
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine_no_s3.get_batch_data(
            batch_spec=S3BatchSpec(
                s3=full_path,
                reader_method="read_csv",
                splitter_method="_split_on_whole_table",
            )
        )


def test_get_batch_with_split_on_column_value(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "batch_id",
                "partition_definition": {"batch_id": 2},
            },
        )
    )
    assert split_df.shape == (12, 10)
    assert (split_df.batch_id == 2).all()

    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "date",
                "partition_definition": {"date": datetime.date(2020, 1, 30)},
            },
        )
    )
    assert (split_df).shape == (3, 10)


def test_get_batch_with_split_on_converted_datetime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_converted_datetime",
            splitter_kwargs={
                "column_name": "timestamp",
                "partition_definition": {"timestamp": "2020-01-30"},
            },
        )
    )
    assert (split_df).shape == (3, 10)


def test_get_batch_with_split_on_divided_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "partition_definition": {"id": 5},
            },
        )
    )
    assert split_df.shape == (10, 10)
    assert split_df.id.min() == 50
    assert split_df.id.max() == 59


def test_get_batch_with_split_on_mod_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_mod_integer",
            splitter_kwargs={
                "column_name": "id",
                "mod": 10,
                "partition_definition": {"id": 5},
            },
        )
    )
    assert split_df.shape == (12, 10)
    assert split_df.id.min() == 5
    assert split_df.id.max() == 115


def test_get_batch_with_split_on_multi_column_values(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
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
    assert split_df.shape == (4, 10)
    assert (split_df.date == datetime.date(2020, 1, 5)).all()

    with pytest.raises(ValueError):
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
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


def test_get_batch_with_split_on_hashed_column(test_df):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                splitter_method="_split_on_hashed_column",
                splitter_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "partition_definition": {
                        "hash_value": "a",
                    },
                    "hash_function_name": "I_am_not_valid",
                },
            )
        )

    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_hashed_column",
            splitter_kwargs={
                "column_name": "favorite_color",
                "hash_digits": 1,
                "partition_definition": {
                    "hash_value": "a",
                },
                "hash_function_name": "sha256",
            },
        )
    )
    assert split_df.shape == (8, 10)


### Sampling methods ###


def test_sample_using_random(test_df):
    random.seed(1)
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_df, sampling_method="_sample_using_random")
    )
    assert sampled_df.shape == (13, 10)


def test_sample_using_mod(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    )
    assert sampled_df.shape == (24, 10)


def test_sample_using_a_list(test_df):
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_a_list",
            sampling_kwargs={
                "column_name": "id",
                "value_list": [3, 5, 7, 11],
            },
        )
    )
    assert sampled_df.shape == (4, 10)


def test_sample_using_md5(test_df):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        sampled_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                sampling_method="_sample_using_hash",
                sampling_kwargs={
                    "column_name": "date",
                    "hash_function_name": "I_am_not_valid",
                },
            )
        )

    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            sampling_method="_sample_using_hash",
            sampling_kwargs={"column_name": "date", "hash_function_name": "md5"},
        )
    )
    assert sampled_df.shape == (10, 10)
    assert sampled_df.date.isin(
        [
            datetime.date(2020, 1, 15),
            datetime.date(2020, 1, 29),
        ]
    ).all()


### Splitting + Sampling methods ###
def test_get_batch_with_split_on_divided_integer_and_sample_on_list(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "partition_definition": {"id": 5},
            },
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    )
    assert split_df.shape == (2, 10)
    assert split_df.id.min() == 54
    assert split_df.id.max() == 59
