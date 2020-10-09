import logging

import boto3
import pandas as pd
import pytest
from moto import mock_s3

from great_expectations.datasource.batch_kwargs_generator.s3_batch_kwargs_generator import (
    S3GlobReaderBatchKwargsGenerator,
)
from great_expectations.exceptions import BatchKwargsError


@pytest.fixture(scope="module")
def mock_s3_bucket():
    with mock_s3():
        bucket = "test_bucket"

        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=bucket)
        client = boto3.client("s3", region_name="us-east-1")

        df = pd.DataFrame({"c1": [1, 2, 3], "c2": ["a", "b", "c"]})
        keys = [
            "data/for/you.csv",
            "data/for/me.csv",
            "data/to/you.csv",
            "other/to/you.csv",
            "other/for/you.csv",
            "other/is/you.csv",
            "delta_files/blarg.parquet",
            "data/is/you.csv",
        ]
        for key in keys:
            client.put_object(
                Bucket=bucket, Body=df.to_csv(index=None).encode("utf-8"), Key=key
            )
        yield bucket


@pytest.fixture
def s3_generator(mock_s3_bucket, basic_sparkdf_datasource):
    # We configure a generator that will fetch from (mocked) my_bucket
    # and will use glob patterns to match returned assets into batches of the same asset
    generator = S3GlobReaderBatchKwargsGenerator(
        "my_generator",
        datasource=basic_sparkdf_datasource,
        bucket=mock_s3_bucket,
        reader_options={"sep": ","},
        assets={
            "data": {
                "prefix": "data/",
                "delimiter": "",
                "partition_regex": r"data/for/(.*)\.csv",
                "regex_filter": r"data/for/.*\.csv",
            },
            "data_dirs": {"prefix": "data/", "directory_assets": True},
            "other": {
                "prefix": "other/",
                "regex_filter": r".*/you\.csv",
                "reader_options": {"sep": "\t"},
            },
            "delta_files": {
                "prefix": "delta_files/",
                "regex_filter": r".*/blarg\.parquet",
                "reader_method": "delta",
            },
            "other_empty_delimiter": {
                "prefix": "other/",
                "delimiter": "",
                "regex_filter": r".*/you\.csv",
                "reader_options": {"sep": "\t"},
                "max_keys": 1,
            },
            "dir": {"prefix": "data/", "directory_assets": True,},
            "dir_misconfigured": {"prefix": "data/", "directory_assets": False,},
        },
    )
    yield generator


def test_s3_generator_basic_operation(s3_generator):
    # S3 Generator sees *only* configured assets
    assets = s3_generator.get_available_data_asset_names()
    assert set(assets["names"]) == {
        ("data", "file"),
        ("data_dirs", "file"),
        ("other", "file"),
        ("delta_files", "file"),
        ("other_empty_delimiter", "file"),
        ("dir_misconfigured", "file"),
        ("dir", "file"),
    }

    # We should observe that glob, prefix, delimiter all work together
    # They can be defined in the generator or overridden by a particular asset
    # Under the hood, we use the S3 ContinuationToken options to lazily fetch data
    batch_kwargs = [
        kwargs for kwargs in s3_generator.get_iterator(data_asset_name="data")
    ]
    assert len(batch_kwargs) == 2
    assert batch_kwargs[0]["reader_options"]["sep"] == ","
    assert batch_kwargs[0]["s3"] in [
        "s3a://test_bucket/data/for/you.csv",
        "s3a://test_bucket/data/for/me.csv",
    ]

    # When a prefix and delimiter do not yield objects, there are no objects returned; raise an error
    with pytest.raises(BatchKwargsError) as err:
        batch_kwargs = [
            kwargs for kwargs in s3_generator.get_iterator(data_asset_name="other")
        ]
        # The error should show the common prefixes
    assert "common_prefixes" in err.value.batch_kwargs


def test_s3_generator_incremental_fetch(s3_generator, caplog):
    caplog.set_level(
        logging.DEBUG,
        logger="great_expectations.datasource.batch_kwargs_generator.s3_batch_kwargs_generator",
    )

    # When max_keys is not set, it defaults to 1000, so all items are returned in the first iterator batch,
    # causing only one fetch (and one log entry referencing the startup of the method)
    caplog.clear()
    batch_kwargs = [
        kwargs for kwargs in s3_generator.get_iterator(data_asset_name="data")
    ]
    assert len(caplog.records) == 2
    assert len(batch_kwargs) == 2

    # When there are more items to return than the S3 API returns, we exhaust our iterator and refetch
    # The result is a new log record for each fetch (plus one for entering the method).
    # Since there are three assets matched by 'other_empty_delimiter' we create four additional log entries with three
    # refetch operations
    caplog.clear()
    batch_kwargs = [
        kwargs
        for kwargs in s3_generator.get_iterator(data_asset_name="other_empty_delimiter")
    ]
    assert len(caplog.records) == 4
    assert len(batch_kwargs) == 3


def test_s3_generator_get_directories(s3_generator):
    # Verify that an asset configured to return directories can do so
    batch_kwargs_list = [
        kwargs for kwargs in s3_generator.get_iterator(data_asset_name="data_dirs")
    ]
    assert 3 == len(batch_kwargs_list)
    paths = {batch_kwargs["s3"] for batch_kwargs in batch_kwargs_list}
    assert {
        "s3a://test_bucket/data/for/",
        "s3a://test_bucket/data/to/",
        "s3a://test_bucket/data/is/",
    } == paths


def test_s3_generator_limit(s3_generator):
    batch_kwargs_list = [
        kwargs for kwargs in s3_generator.get_iterator(data_asset_name="data", limit=10)
    ]
    assert all(["limit" in batch_kwargs for batch_kwargs in batch_kwargs_list])


def test_s3_generator_reader_method_configuration(s3_generator):
    batch_kwargs_list = [
        kwargs
        for kwargs in s3_generator.get_iterator(data_asset_name="delta_files", limit=10)
    ]
    assert batch_kwargs_list[0]["reader_method"] == "delta"


def test_s3_generator_build_batch_kwargs_no_partition_id(s3_generator):
    batch_kwargs = s3_generator.build_batch_kwargs("data")
    assert batch_kwargs["s3"] in [
        "s3a://test_bucket/data/for/you.csv",
        "s3a://test_bucket/data/for/me.csv",
    ]


def test_s3_generator_build_batch_kwargs_partition_id(s3_generator):
    batch_kwargs = s3_generator.build_batch_kwargs("data", "you")
    assert batch_kwargs["s3"] == "s3a://test_bucket/data/for/you.csv"


def test_s3_generator_directory_asset(s3_generator):
    batch_kwargs = s3_generator.build_batch_kwargs("dir")
    assert batch_kwargs["s3"] in [
        "s3a://test_bucket/data/for/",
        "s3a://test_bucket/data/to/",
    ]


def test_s3_generator_misconfigured_directory_asset(s3_generator):
    with pytest.raises(BatchKwargsError) as exc:
        _ = s3_generator.build_batch_kwargs("dir_misconfigured")
    assert "The asset may not be configured correctly." in str(exc.value)


def test_s3_get_available_partition_ids(s3_generator):
    assert s3_generator.get_available_partition_ids(data_asset_name="data") == [
        "me",
        "you",
    ]
