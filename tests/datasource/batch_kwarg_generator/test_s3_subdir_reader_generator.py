import logging
import os
import time

import pandas as pd
import pytest
import requests
from botocore.session import Session

from great_expectations.datasource.batch_kwargs_generator import (
    S3SubdirReaderBatchKwargsGenerator,
)
from great_expectations.exceptions import BatchKwargsError

port = 5555
endpoint_uri = "http://127.0.0.1:%s/" % port
os.environ["AWS_ACCESS_KEY_ID"] = "dummy_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "dummy_secret"


@pytest.fixture(scope="module")
def s3_base():
    # writable local S3 system
    import shlex
    import subprocess

    proc = subprocess.Popen(shlex.split("moto_server s3 -p %s" % port))

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(endpoint_uri)
            if r.ok:
                break
        except:
            pass
        timeout -= 0.1
        time.sleep(0.1)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="module")
def mock_s3_bucket(s3_base):
    bucket = "test_bucket"
    session = Session()
    client = session.create_client("s3", endpoint_url=endpoint_uri)
    client.create_bucket(Bucket=bucket, ACL="public-read")

    df = pd.DataFrame({"c1": [1, 2, 3], "c2": ["a", "b", "c"]})
    keys = [
        "data/for/you.csv",
        "data/for/me.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=df.to_csv(index=None).encode("utf-8"), Key=key
        )
    yield bucket


@pytest.fixture
def s3_subdir_generator(mock_s3_bucket, basic_sparkdf_datasource):
    # We configure a generator that will fetch from (mocked) my_bucket
    # and will use glob patterns to match returned assets into batches of the same asset
    generator = S3SubdirReaderBatchKwargsGenerator(
        "my_generator",
        datasource=basic_sparkdf_datasource,
        boto3_options={"endpoint_url": endpoint_uri},
        base_directory="test_bucket/data/for",
        reader_options={"sep": ","},
    )
    yield generator


@pytest.fixture
def s3_subdir_generator_with_partition(mock_s3_bucket, basic_sparkdf_datasource):
    # We configure a generator that will fetch from (mocked) my_bucket
    # and will use glob patterns to match returned assets into batches of the same asset
    generator = S3SubdirReaderBatchKwargsGenerator(
        "my_generator",
        datasource=basic_sparkdf_datasource,
        boto3_options={"endpoint_url": endpoint_uri},
        base_directory="test_bucket/data/",
        reader_options={"sep": ","},
    )
    yield generator


def test_s3_subdir_generator_basic_operation(s3_subdir_generator):
    # S3 Generator sees *only* configured assets
    assets = s3_subdir_generator.get_available_data_asset_names()
    print(assets)
    assert set(assets["names"]) == {
        ("you", "file"),
        ("me", "file"),
    }


def test_s3_subdir_generator_reader_options_configuration(s3_subdir_generator):
    batch_kwargs_list = [
        kwargs
        for kwargs in s3_subdir_generator.get_iterator(data_asset_name="you", limit=10)
    ]
    print(batch_kwargs_list)
    assert batch_kwargs_list[0]["reader_options"] == {"sep": ","}


def test_s3_subdir_generator_build_batch_kwargs_no_partition_id(s3_subdir_generator):
    batch_kwargs = s3_subdir_generator.build_batch_kwargs("you")
    assert batch_kwargs["s3"] in [
        "s3a://test_bucket/data/for/you.csv",
    ]


def test_s3_subdir_generator_build_batch_kwargs_partition_id(
    s3_subdir_generator_with_partition, basic_sparkdf_datasource
):

    batch_kwargs = s3_subdir_generator_with_partition.build_batch_kwargs("for", "you")
    assert batch_kwargs["s3"] == "s3a://test_bucket/data/for/you.csv"
