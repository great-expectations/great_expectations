import pytest
from contextlib import contextmanager
from moto import mock_s3

from great_expectations.core.logging import S3_TELEMETRY_BUCKET

import boto3

import logging
from aws_logging_handlers.S3 import S3Handler


@contextmanager
def logging_wrapper():
    yield True
    logging.shutdown()


@pytest.fixture(scope="module")
def logging_bucket():
    bucket = S3_TELEMETRY_BUCKET
    with mock_s3():
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)
        yield bucket


def test_basic_telemetry(logging_bucket):
    with logging_wrapper():
        logger = logging.getLogger("great_expectations.test.module")
        s3_handler = S3Handler("great_expectations", logging_bucket, compress=True)
        logger.addHandler(s3_handler)
        logger.critical("test")

    conn = boto3.client("s3", region_name='us-east-1')
    res = conn.list_objects(Bucket=logging_bucket)
    assert 1 == len(res["Contents"])
