import pytest
from moto import mock_s3

import logging
import pandas as pd
import boto3

from great_expectations.datasource.generator.s3_generator import S3Generator
from great_expectations.exceptions import BatchKwargsError


@pytest.fixture(scope="module")
def mock_s3_bucket():
    with mock_s3():
        bucket = 'test_bucket'

        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=bucket)
        client = boto3.client('s3', region_name='us-east-1')

        df = pd.DataFrame({"c1": [1, 2, 3], "c2": ["a", "b", "c"]})
        keys = [
            "data/for/you.csv",
            "data/for/me.csv",
            "data/to/you.csv",
            "other/to/you.csv",
            "other/for/you.csv",
            "other/is/you.csv",
            "data/is/you.csv"
        ]
        for key in keys:
            client.put_object(
                Bucket=bucket,
                Body=df.to_csv(index=None).encode('utf-8'),
                Key=key
            )
        yield bucket


@pytest.fixture
def s3_generator(mock_s3_bucket):
    # We configure a generator that will fetch from (mocked) my_bucket
    # and will use glob patterns to match returned assets into batches of the same asset
    generator = S3Generator("my_generator",
                            datasource=None,
                            bucket=mock_s3_bucket,
                            reader_options={
                                "sep": ","
                            },
                            assets={
                                "data": {
                                    "prefix": "data/",
                                    "delimiter": "",
                                    "regex_filter": r"data/for/.*\.csv"
                                },
                                "other": {
                                    "prefix": "other/",
                                    "regex_filter": r".*/you\.csv",
                                    "reader_options": {
                                        "sep": "\t"
                                    }
                                },
                                "other_empty_delimiter": {
                                    "prefix": "other/",
                                    "delimiter": "",
                                    "regex_filter": r".*/you\.csv",
                                    "reader_options": {
                                        "sep": "\t"
                                    },
                                    "max_keys": 1
                                }
                            }
                            )
    yield generator


def test_s3_generator_basic_operation(s3_generator):
    # S3 Generator sees *only* configured assets
    assets = s3_generator.get_available_data_asset_names()
    assert set(assets) == {"data", "other", "other_empty_delimiter"}

    # We should observe that glob, prefix, delimiter all work together
    # They can be defined in the generator or overridden by a particular asset
    # Under the hood, we use the S3 ContinuationToken options to lazily fetch data
    batch_kwargs = [kwargs for kwargs in s3_generator.get_iterator("data")]
    assert len(batch_kwargs) == 2
    assert batch_kwargs[0]["sep"] == ","
    assert batch_kwargs[0]["s3"] in ["s3a://test_bucket/data/for/you.csv", "s3a://test_bucket/data/for/me.csv"]

    # When a prefix and delimiter do not yield objects, there are no objects returned; raise an error
    with pytest.raises(BatchKwargsError) as err:
        batch_kwargs = [kwargs for kwargs in s3_generator.get_iterator("other")]
        # The error should show the common prefixes
        assert "CommonPrefixes" in err.batch_kwargs


def test_s3_generator_incremental_fetch(s3_generator, caplog):
    caplog.set_level(logging.DEBUG, logger="great_expectations.datasource.generator.s3_generator")

    # When max_keys is not set, it defaults to 1000, so all items are returned in the first iterator batch,
    # causing only one fetch (and one log entry referencing the startup of the method)
    caplog.clear()
    batch_kwargs = [kwargs for kwargs in s3_generator.get_iterator("data")]
    assert len(caplog.records) == 2
    assert len(batch_kwargs) == 2

    # When there are more items to return than the S3 API returns, we exhaust our iterator and refetch
    # The result is a new log record for each fetch (plus one for entering the method).
    # Since there are three assets matched by 'other_empty_delimiter' we create four additional log entries with three
    # refetch operations
    caplog.clear()
    batch_kwargs = [kwargs for kwargs in s3_generator.get_iterator("other_empty_delimiter")]
    assert len(caplog.records) == 4
    assert len(batch_kwargs) == 3
