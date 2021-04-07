import datetime
import os
import random
from pathlib import Path
from typing import List

import boto3
import pandas as pd
import pytest
from botocore.errorfactory import ClientError
from moto import mock_s3

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import (
    PathBatchSpec,
    RuntimeDataBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.data_connector import ConfiguredAssetS3DataConnector
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


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

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
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
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.nonexistent",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)

    with pytest.raises(MetricProviderError) as e:
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


def test_get_batch_with_split_on_whole_table(test_df):
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


def test_get_batch_with_split_on_whole_table_s3(
    batch_with_split_on_whole_table_s3, test_df_small
):
    df = PandasExecutionEngine().get_batch_data(
        batch_spec=batch_with_split_on_whole_table_s3
    )
    assert df.dataframe.shape == test_df_small.shape


def test_get_batch_with_no_s3_configured(batch_with_split_on_whole_table_s3):
    # if S3 was not configured
    execution_engine_no_s3 = PandasExecutionEngine()
    execution_engine_no_s3._s3 = None
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        execution_engine_no_s3.get_batch_data(
            batch_spec=batch_with_split_on_whole_table_s3
        )


def test_get_batch_with_split_on_whole_table_s3_with_configured_asset_s3_data_connector(
    test_s3_files, test_df_small
):
    bucket, _keys = test_s3_files
    expected_df = test_df_small

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
    test_df = PandasExecutionEngine().get_batch_data(
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
    with pytest.raises(ClientError):
        PandasExecutionEngine().get_batch_data(
            batch_spec=my_data_connector.build_batch_spec(
                batch_definition=batch_def_no_key
            )
        )


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


def test_get_batch_s3_compressed_files(test_s3_files_compressed, test_df_small):
    bucket, keys = test_s3_files_compressed
    path = keys[0]
    full_path = f"s3a://{os.path.join(bucket, path)}"

    batch_spec = S3BatchSpec(path=full_path, reader_method="read_csv")
    df = PandasExecutionEngine().get_batch_data(batch_spec=batch_spec)
    assert df.dataframe.shape == test_df_small.shape


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
