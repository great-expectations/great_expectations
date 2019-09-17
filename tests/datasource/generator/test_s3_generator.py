from great_expectations.datasource.generator.s3_generator import S3Generator

try:
    from unittest import mock
except ImportError:
    import mock


class MockS3(object):
    def list_objects_v2(self, *args, ContinuationToken=None, **kwargs):
        """MOCK"""
        if ContinuationToken is None:
            return {
                "IsTruncated": True,
                "Contents": [
                    {
                        "Key": "data/for/you.csv"
                    },
                    {
                        "Key": "data/to/you.csv"
                    },
                    {
                        "Key": "other/to/you.csv"
                    },
                ],
                "NextContinuationToken": "getmoredataz"
            }
        else:
            return {
                "IsTruncated": False,
                "Contents": [
                    {
                        "Key": "other/for/you.csv"
                    },
                    {
                        "Key": "data/is/you.csv"
                    }
                ]
            }


def test_s3_generator_basic_operation():

    with mock.patch("boto3.client") as mock_client:
        mock_client.return_value = MockS3()

        # We configure a generator that will fetch from (mocked) my_bucket
        # and will use glob patterns to match returned assets into batches of the same asset
        generator = S3Generator("my_generator",
                                datasource=None,
                                bucket="my_bucket",
                                reader_options={
                                    "sep": ","
                                },
                                assets={
                                    "data": {
                                        "prefix": "",
                                        "delimiter": "/",
                                        "glob": "*/for/you.csv"
                                    },
                                    "other": {
                                        "prefix": "",
                                        "glob": "*/to/you.csv",
                                        "reader_options": {
                                            "sep": "\t"
                                        }
                                    }
                                }
                                )

        # S3 Generator sees *only* configured assets
        assets = generator.get_available_data_asset_names()
        assert assets == {"data", "other"}

        # We should observe that glob, prefix, delimiter all work together
        # They can be defined in the generator or overridden by a particular asset
        # Under the hood, we use the S3 ContinuationToken options to lazily fetch data
        batch_kwargs = [kwargs for kwargs in generator.get_iterator("data")]
        assert len(batch_kwargs) == 2
        assert batch_kwargs[0]["sep"] == ","
        assert batch_kwargs[0]["s3"] == "s3a://my_bucket/data/for/you.csv"

        batch_kwargs = [kwargs for kwargs in generator.get_iterator("other")]
        assert len(batch_kwargs) == 2
        assert batch_kwargs[0]["sep"] == "\t"
