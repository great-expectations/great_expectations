import pytest
from moto import mock_s3

import pandas as pd
import boto3

from great_expectations.datasource.generator.s3_generator import S3Generator
from great_expectations.exceptions import BatchKwargsError


@mock_s3
def test_s3_generator_basic_operation():

    # with mock.patch("boto3.client") as mock_client:
    #     mock_client.return_value = MockS3()
    bucket = 'test_bucket'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket)
    client = boto3.client('s3', region_name='us-east-1')

    df = pd.DataFrame({"c1": [1, 2, 3], "c2": ["a", "b", "c"]})
    keys = [
        "data/for/you.csv",
        "data/to/you.csv",
        "other/to/you.csv",
        "other/for/you.csv",
        "data/is/you.csv"
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket,
            Body=df.to_csv(index=None).encode('utf-8'),
            Key=key
        )

    # We configure a generator that will fetch from (mocked) my_bucket
    # and will use glob patterns to match returned assets into batches of the same asset
    generator = S3Generator("my_generator",
                            datasource=None,
                            bucket=bucket,
                            reader_options={
                                "sep": ","
                            },
                            assets={
                                "data": {
                                    "prefix": "data/",
                                    "delimiter": "",
                                    "regex_filter": ".*/for/you.csv"
                                },
                                "other": {
                                    "prefix": "other/",
                                    "regex_filter": ".*/you.csv",
                                    "reader_options": {
                                        "sep": "\t"
                                    }
                                },
                                "other_empty_delimiter": {
                                    "prefix": "other/",
                                    "delimiter": "",
                                    "regex_filter": ".*/you.csv",
                                    "reader_options": {
                                        "sep": "\t"
                                    },
                                    "max_keys": 1
                                }
                            }
                            )

    # S3 Generator sees *only* configured assets
    assets = generator.get_available_data_asset_names()
    assert assets == {"data", "other", "other_empty_delimiter"}

    # We should observe that glob, prefix, delimiter all work together
    # They can be defined in the generator or overridden by a particular asset
    # Under the hood, we use the S3 ContinuationToken options to lazily fetch data
    batch_kwargs = [kwargs for kwargs in generator.get_iterator("data")]
    assert len(batch_kwargs) == 1
    assert batch_kwargs[0]["sep"] == ","
    assert batch_kwargs[0]["s3"] == "s3a://test_bucket/data/for/you.csv"

    # When a prefix and delimiter do not yield objects, there are no objects returned; raise an error
    with pytest.raises(BatchKwargsError) as err:
        batch_kwargs = [kwargs for kwargs in generator.get_iterator("other")]
        assert "CommonPrefixes" in err.batch_kwargs

    # We should be able to fetch in small batches and still work fine
    batch_kwargs = [kwargs for kwargs in generator.get_iterator("other_empty_delimiter")]
    assert len(batch_kwargs) == 2
    assert batch_kwargs[0]["sep"] == "\t"

