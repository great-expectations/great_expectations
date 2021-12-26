import datetime
import os
import random
from pathlib import Path
from typing import List
from unittest import mock

import boto3
import pandas as pd
import pytest
from moto import mock_s3

try:
    from azure.storage.blob import BlobServiceClient
except:
    azure = None


import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import (
    AzureBatchSpec,
    GCSBatchSpec,
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.data_connector import ConfiguredAssetS3DataConnector
from great_expectations.execution_engine.execution_engine import (
    ExecutionEngine,
    MetricDomainTypes,
)
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_bucket(s3):
    bucket: str = "test_bucket"
    s3.create_bucket(Bucket=bucket)
    return bucket


@pytest.fixture
def test_df_small() -> pd.DataFrame:
    return pd.DataFrame(data={"col1": [1, 0, 505], "col2": [3, 4, 101]})


@pytest.fixture
def test_df_small_csv_compressed(test_df_small, tmpdir) -> bytes:
    path = Path(tmpdir) / "file.csv.gz"
    test_df_small.to_csv(path, index=False, compression="gzip")
    return path.read_bytes()


@pytest.fixture
def test_df_small_csv(test_df_small, tmpdir) -> bytes:
    path = Path(tmpdir) / "file.csv"
    test_df_small.to_csv(path, index=False)
    return path.read_bytes()


@pytest.fixture
def test_s3_files(s3, s3_bucket, test_df_small_csv):
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    for key in keys:
        s3.put_object(Bucket=s3_bucket, Body=test_df_small_csv, Key=key)
    return s3_bucket, keys


@pytest.fixture
def test_s3_files_parquet(tmpdir, s3, s3_bucket, test_df_small, test_df_small_csv):
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.parquet",
        "directory/B-2.parquet",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = Path(tmpdir) / "file.parquet"
    test_df_small.to_parquet(path)
    for key in keys:
        if key.endswith(".parquet"):
            with open(path, "rb") as f:
                s3.put_object(Bucket=s3_bucket, Body=f, Key=key)
        else:
            s3.put_object(Bucket=s3_bucket, Body=test_df_small_csv, Key=key)
    return s3_bucket, keys


@pytest.fixture
def batch_with_split_on_whole_table_s3(test_s3_files) -> S3BatchSpec:
    bucket, keys = test_s3_files
    path = keys[0]
    full_path = f"s3a://{os.path.join(bucket, path)}"

    batch_spec = S3BatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


@pytest.fixture
def test_s3_files_compressed(s3, s3_bucket, test_df_small_csv_compressed):
    keys: List[str] = [
        "path/A-100.csv.gz",
        "path/A-101.csv.gz",
        "directory/B-1.csv.gz",
        "directory/B-2.csv.gz",
    ]

    for key in keys:
        s3.put_object(
            Bucket=s3_bucket,
            Body=test_df_small_csv_compressed,
            Key=key,
        )
    return s3_bucket, keys


@pytest.fixture
def azure_batch_spec() -> AzureBatchSpec:
    container = "test_container"
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = keys[0]
    full_path = os.path.join("mock_account.blob.core.windows.net", container, path)

    batch_spec = AzureBatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


@pytest.fixture
def gcs_batch_spec() -> GCSBatchSpec:
    bucket = "test_bucket"
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = keys[0]
    full_path = os.path.join("gs://", bucket, path)

    batch_spec = GCSBatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


def test_constructor_with_boto3_options():
    # default instantiation
    PandasExecutionEngine()

    # instantiation with custom parameters
    engine = PandasExecutionEngine(discard_subset_failing_expectations=True)
    assert "discard_subset_failing_expectations" in engine.config
    assert engine.config.get("discard_subset_failing_expectations") is True
    custom_boto3_options = {"region_name": "us-east-1"}
    engine = PandasExecutionEngine(boto3_options=custom_boto3_options)
    assert "boto3_options" in engine.config
    assert engine.config.get("boto3_options")["region_name"] == "us-east-1"


def test_reader_fn():
    engine = PandasExecutionEngine()

    # Testing that can recognize basic excel file
    fn = engine._get_reader_fn(path="myfile.xlsx")
    assert "<function read_excel" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader_method="read_csv")
    assert "<function" in str(fn_new)


def test_get_domain_records_with_column_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": "b<5",
            "condition_parser": "pandas",
        }
    )

    expected_column_df = df.iloc[:3]

    assert data.equals(
        expected_column_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_column_pair_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "row_condition": "b>2",
            "condition_parser": "pandas",
            "ignore_row_if": "both_values_are_missing",
        }
    )

    expected_column_pair_df = pd.DataFrame(
        {
            "a": [2, 3, 4, 6],
            "b": [3.0, 4.0, 5.0, 6.0],
            "c": [2.0, 3.0, 4.0, None],
        },
        index=[1, 2, 3, 5],
    )
    assert data.equals(
        expected_column_pair_df
    ), "Data does not match after getting full access compute domain"

    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": "b>2",
            "condition_parser": "pandas",
            "ignore_row_if": "either_value_is_missing",
        }
    )
    data = data.astype(int)

    expected_column_pair_df = pd.DataFrame(
        {"a": [2, 3, 4], "b": [3, 4, 5], "c": [2, 3, 4]}, index=[1, 2, 3]
    )

    assert data.equals(
        expected_column_pair_df
    ), "Data does not match after getting full access compute domain"

    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": "a<6",
            "condition_parser": "pandas",
            "ignore_row_if": "neither",
        }
    )

    expected_column_pair_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [2.0, 3.0, 4.0, 5.0, None],
            "c": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )

    assert data.equals(
        expected_column_pair_df
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_multicolumn_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["a", "c"],
            "row_condition": "b>2",
            "condition_parser": "pandas",
            "ignore_row_if": "all_values_are_missing",
        }
    )
    data = data.astype(int)

    expected_multicolumn_df = pd.DataFrame(
        {"a": [2, 3, 4, 5], "b": [3, 4, 5, 7], "c": [2, 3, 4, 6]}, index=[1, 2, 3, 5]
    )

    assert data.equals(
        expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"

    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "row_condition": "a<5",
            "condition_parser": "pandas",
            "ignore_row_if": "any_value_is_missing",
        }
    )
    data = data.astype(int)

    expected_multicolumn_df = pd.DataFrame(
        {"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [1, 2, 3, 4]}, index=[0, 1, 2, 3]
    )

    assert data.equals(
        expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"

    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "ignore_row_if": "never",
        }
    )

    expected_multicolumn_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        },
        index=[0, 1, 2, 3, 4, 5],
    )

    assert data.equals(
        expected_multicolumn_df
    ), "Data does not match after getting full access compute domain"


def test_get_compute_domain_with_no_domain_kwargs():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="table"
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
    expected_column_pair_df = df.drop(columns=["c"])

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


def test_get_compute_domain_with_multicolumn_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 2, 3], "d": [2, 7, 9, 2]}
    )

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_list": ["a", "b", "c"]}, domain_type="multicolumn"
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_list": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df["b"] > 2]

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
    expected_df = df[df["b"] > 24]

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "a",
            "row_condition": "b > 24",
            "condition_parser": "pandas",
        },
        domain_type="column",
    )
    # Ensuring data has been properly queried
    assert data["b"].equals(
        expected_df["b"]
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# Just checking that the Pandas Execution Engine can perform these in sequence
def test_resolve_metric_bundle():
    df = pd.DataFrame({"a": [1, 2, 3, None]})

    # Building engine and configurations in attempt to resolve metrics
    engine = PandasExecutionEngine(batch_data_dict={"made-up-id": df})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

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
        metric_value_kwargs=None,
    )
    stdev = MetricConfiguration(
        metric_name="column.nonexistent",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metrics = (mean, stdev)

    with pytest.raises(ge_exceptions.MetricProviderError) as e:
        # noinspection PyUnusedLocal
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
    assert split_df.dataframe.shape == (120, 10)

    # No dataset passed to RuntimeDataBatchSpec
    with pytest.raises(ge_exceptions.InvalidBatchSpecError):
        PandasExecutionEngine().get_batch_data(RuntimeDataBatchSpec())


def test_get_batch_with_split_on_whole_table_runtime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df, splitter_method="_split_on_whole_table"
        )
    )
    assert split_df.dataframe.shape == (120, 10)


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
    assert test_df.dataframe.shape == (5, 2)


def test_get_batch_with_split_on_whole_table_s3(
    batch_with_split_on_whole_table_s3, test_df_small
):
    df = PandasExecutionEngine().get_batch_data(
        batch_spec=batch_with_split_on_whole_table_s3
    )
    assert df.dataframe.shape == test_df_small.shape


def test_get_batch_with_split_on_whole_table_s3_with_configured_asset_s3_data_connector(
    test_s3_files, test_df_small
):
    bucket, _keys = test_s3_files
    expected_df = test_df_small

    execution_engine: ExecutionEngine = PandasExecutionEngine()

    my_data_connector = ConfiguredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        bucket=bucket,
        execution_engine=execution_engine,
        prefix="",
        assets={"alpha": {}},
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
    )
    batch_def = BatchDefinition(
        datasource_name="FAKE_DATASOURCE_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        batch_identifiers=IDDict(index=1),
        batch_spec_passthrough={
            "reader_method": "read_csv",
            "splitter_method": "_split_on_whole_table",
        },
    )
    test_df = execution_engine.get_batch_data(
        batch_spec=my_data_connector.build_batch_spec(batch_definition=batch_def)
    )
    assert test_df.dataframe.shape == expected_df.shape

    # if key does not exist
    batch_def_no_key = BatchDefinition(
        datasource_name="FAKE_DATASOURCE_NAME",
        data_connector_name="my_data_connector",
        data_asset_name="alpha",
        batch_identifiers=IDDict(index=9),
        batch_spec_passthrough={
            "reader_method": "read_csv",
            "splitter_method": "_split_on_whole_table",
        },
    )
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine.get_batch_data(
            batch_spec=my_data_connector.build_batch_spec(
                batch_definition=batch_def_no_key
            )
        )


def test_get_batch_s3_compressed_files(test_s3_files_compressed, test_df_small):
    bucket, keys = test_s3_files_compressed
    path = keys[0]
    full_path = f"s3a://{os.path.join(bucket, path)}"

    batch_spec = S3BatchSpec(path=full_path, reader_method="read_csv")
    df = PandasExecutionEngine().get_batch_data(batch_spec=batch_spec)
    assert df.dataframe.shape == test_df_small.shape


def test_get_batch_s3_parquet(test_s3_files_parquet, test_df_small):
    bucket, keys = test_s3_files_parquet
    path = [key for key in keys if key.endswith(".parquet")][0]
    full_path = f"s3a://{os.path.join(bucket, path)}"

    batch_spec = S3BatchSpec(path=full_path, reader_method="read_parquet")
    df = PandasExecutionEngine().get_batch_data(batch_spec=batch_spec)
    assert df.dataframe.shape == test_df_small.shape


def test_get_batch_with_no_s3_configured():
    batch_spec = S3BatchSpec(
        path="s3a://i_dont_exist",
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    # if S3 was not configured
    execution_engine_no_s3 = PandasExecutionEngine()

    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine_no_s3.get_batch_data(batch_spec=batch_spec)


def test_get_batch_with_split_on_column_value(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "batch_id",
                "batch_identifiers": {"batch_id": 2},
            },
        )
    )
    assert split_df.dataframe.shape == (12, 10)
    assert (split_df.dataframe.batch_id == 2).all()

    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_column_value",
            splitter_kwargs={
                "column_name": "date",
                "batch_identifiers": {"date": datetime.date(2020, 1, 30)},
            },
        )
    )
    assert split_df.dataframe.shape == (3, 10)


def test_get_batch_with_split_on_converted_datetime(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_converted_datetime",
            splitter_kwargs={
                "column_name": "timestamp",
                "batch_identifiers": {"timestamp": "2020-01-30"},
            },
        )
    )
    assert split_df.dataframe.shape == (3, 10)


def test_get_batch_with_split_on_divided_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_divided_integer",
            splitter_kwargs={
                "column_name": "id",
                "divisor": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert split_df.dataframe.shape == (10, 10)
    assert split_df.dataframe.id.min() == 50
    assert split_df.dataframe.id.max() == 59


def test_get_batch_with_split_on_mod_integer(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
            splitter_method="_split_on_mod_integer",
            splitter_kwargs={
                "column_name": "id",
                "mod": 10,
                "batch_identifiers": {"id": 5},
            },
        )
    )
    assert split_df.dataframe.shape == (12, 10)
    assert split_df.dataframe.id.min() == 5
    assert split_df.dataframe.id.max() == 115


def test_get_batch_with_split_on_multi_column_values(test_df):
    split_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(
            batch_data=test_df,
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
    )
    assert split_df.dataframe.shape == (4, 10)
    assert (split_df.dataframe.date == datetime.date(2020, 1, 5)).all()

    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
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
        )


def test_get_batch_with_split_on_hashed_column(test_df):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
        split_df = PandasExecutionEngine().get_batch_data(
            RuntimeDataBatchSpec(
                batch_data=test_df,
                splitter_method="_split_on_hashed_column",
                splitter_kwargs={
                    "column_name": "favorite_color",
                    "hash_digits": 1,
                    "batch_identifiers": {
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
                "batch_identifiers": {
                    "hash_value": "a",
                },
                "hash_function_name": "sha256",
            },
        )
    )
    assert split_df.dataframe.shape == (8, 10)


### Sampling methods ###


def test_sample_using_random(test_df):
    random.seed(1)
    sampled_df = PandasExecutionEngine().get_batch_data(
        RuntimeDataBatchSpec(batch_data=test_df, sampling_method="_sample_using_random")
    )
    assert sampled_df.dataframe.shape == (13, 10)


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
    assert sampled_df.dataframe.shape == (24, 10)


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
    assert sampled_df.dataframe.shape == (4, 10)


def test_sample_using_md5(test_df):
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        # noinspection PyUnusedLocal
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
    assert sampled_df.dataframe.shape == (10, 10)
    assert sampled_df.dataframe.date.isin(
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
                "batch_identifiers": {"id": 5},
            },
            sampling_method="_sample_using_mod",
            sampling_kwargs={
                "column_name": "id",
                "mod": 5,
                "value": 4,
            },
        )
    )
    assert split_df.dataframe.shape == (2, 10)
    assert split_df.dataframe.id.min() == 54
    assert split_df.dataframe.id.max() == 59


@mock.patch(
    "great_expectations.execution_engine.pandas_execution_engine.BlobServiceClient",
)
def test_constructor_with_azure_options(mock_azure_conn):
    # default instantiation
    PandasExecutionEngine()

    # instantiation with custom parameters
    engine = PandasExecutionEngine(discard_subset_failing_expectations=True)
    assert "discard_subset_failing_expectations" in engine.config
    assert engine.config.get("discard_subset_failing_expectations") is True
    custom_azure_options = {"account_url": "my_account_url"}
    engine = PandasExecutionEngine(azure_options=custom_azure_options)
    assert "azure_options" in engine.config
    assert engine.config.get("azure_options")["account_url"] == "my_account_url"


@mock.patch(
    "great_expectations.execution_engine.pandas_execution_engine.BlobServiceClient",
)
def test_get_batch_data_with_azure_batch_spec(
    mock_azure_conn,
    azure_batch_spec,
):
    mock_blob_client = mock_azure_conn().get_blob_client()
    mock_azure_obj = mock_blob_client.download_blob()
    mock_azure_obj.readall.return_value = (
        b"colA,colB,colC\n1,2,3\n4,5,6\n7,8,9"  # (3,3) CSV for testing
    )

    df = PandasExecutionEngine().get_batch_data(batch_spec=azure_batch_spec)

    mock_azure_conn().get_blob_client.assert_called_with(
        container="test_container", blob="path/A-100.csv"
    )
    mock_azure_obj.readall.assert_called_once()

    assert df.dataframe.shape == (3, 3)


def test_get_batch_with_no_azure_configured(azure_batch_spec):
    # if Azure BlobServiceClient was not configured
    execution_engine_no_azure = PandasExecutionEngine()
    execution_engine_no_azure._azure = None

    # Raises error due the connection object not being set
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine_no_azure.get_batch_data(batch_spec=azure_batch_spec)


@mock.patch(
    "great_expectations.execution_engine.pandas_execution_engine.service_account",
)
@mock.patch(
    "great_expectations.execution_engine.pandas_execution_engine.storage.Client",
)
def test_constructor_with_gcs_options(mock_gcs_conn, mock_auth_method):
    # default instantiation
    PandasExecutionEngine()

    # instantiation with custom parameters
    engine = PandasExecutionEngine(discard_subset_failing_expectations=True)
    assert "discard_subset_failing_expectations" in engine.config
    assert engine.config.get("discard_subset_failing_expectations") is True
    custom_gcs_options = {"filename": "a/b/c/my_gcs_credentials.json"}
    engine = PandasExecutionEngine(gcs_options=custom_gcs_options)
    assert "gcs_options" in engine.config
    assert "filename" in engine.config.get("gcs_options")


@mock.patch(
    "great_expectations.execution_engine.pandas_execution_engine.storage.Client",
)
def test_get_batch_data_with_gcs_batch_spec(
    mock_gcs_conn,
    gcs_batch_spec,
):
    mock_gcs_bucket = mock_gcs_conn().get_bucket()
    mock_gcs_blob = mock_gcs_bucket.blob()
    mock_gcs_blob.download_as_bytes.return_value = (
        b"colA,colB,colC\n1,2,3\n4,5,6\n7,8,9"  # (3,3) CSV for testing
    )

    # Necessary to pass kwargs to bypass "os.getenv | gcs_options == {}" check
    kwargs = {"gcs_options": {"my_option": "my_value"}}
    df = PandasExecutionEngine(**kwargs).get_batch_data(batch_spec=gcs_batch_spec)

    mock_gcs_conn().get_bucket.assert_called_with("test_bucket")
    mock_gcs_bucket.blob.assert_called_with("path/A-100.csv")
    mock_gcs_blob.download_as_bytes.assert_called_once()

    assert df.dataframe.shape == (3, 3)


def test_get_batch_data_with_gcs_batch_spec_no_credentials(gcs_batch_spec, monkeypatch):
    # If PandasExecutionEngine contains no credentials for GCS, we will still instantiate _gcs engine,
    # but will raise Exception when trying get_batch_data(). The only situation where it would work is if we are running in a Google Cloud container.
    # TODO : Determine how we can test the scenario where we are running PandasExecutionEngine from within Google Cloud env.

    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    with pytest.raises(Exception):
        PandasExecutionEngine().get_batch_data(batch_spec=gcs_batch_spec)


def test_get_batch_with_gcs_misconfigured(gcs_batch_spec):
    # gcs_batchspec point to data that the ExecutionEngine does not have access to
    execution_engine_no_gcs = PandasExecutionEngine()
    # Raises error if batch_spec causes ExecutionEngine error
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine_no_gcs.get_batch_data(batch_spec=gcs_batch_spec)
