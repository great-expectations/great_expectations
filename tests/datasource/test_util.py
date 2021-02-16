import pandas as pd
import pytest

from great_expectations.datasource.util import S3Url, hash_pandas_dataframe


def test_hash_pandas_dataframe_hashable_df():
    data = [{"col_1": 1}]
    df1 = pd.DataFrame(data)
    df2 = pd.DataFrame(data)
    assert hash_pandas_dataframe(df1) == hash_pandas_dataframe(df2)


def test_hash_pandas_dataframe_unhashable_df():
    data = [{"col_1": {"val": 1}}]
    df1 = pd.DataFrame(data)
    df2 = pd.DataFrame(data)
    assert hash_pandas_dataframe(df1) == hash_pandas_dataframe(df2)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("s3://bucket/hello/world.csv.gz", "gz"),
        ("s3://bucket/hello/world.csv", "csv"),
        ("s3://bucket/hello/world.csv.gz?asd", "gz"),
        ("s3://bucket/hello/world?asd", None),
        ("<scheme>://<netloc>/<path>.csv;<params>?<query>#<fragment>", "csv"),
        ("<scheme>://<netloc>/<path>.;<params>?<query>#<fragment>", None),
    ],
)
def test_s3_suffix(url, expected):
    _suffix = S3Url(url).suffix
    if expected is not None:
        assert _suffix == expected
    else:
        assert _suffix is None
