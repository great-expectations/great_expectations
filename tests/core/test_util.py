import pytest
from freezegun import freeze_time

from great_expectations.core.util import (
    AzureUrl,
    S3Url,
    sniff_s3_compression,
    substitute_all_strftime_format_strings,
)


@freeze_time("11/05/1955")
def test_substitute_all_strftime_format_strings():
    input_dict = {
        "month_no": "%m",
        "just_a_string": "Bloopy!",
        "string_with_month_word": "Today we are in the month %B!",
        "number": "90210",
        "escaped_percent": "'%%m' is the format string for month number",
        "inner_dict": {"day_word_full": "%A"},
        "list": ["a", 123, "%a"],
    }
    expected_output_dict = {
        "month_no": "11",
        "just_a_string": "Bloopy!",
        "string_with_month_word": "Today we are in the month November!",
        "number": "90210",
        "escaped_percent": "'%m' is the format string for month number",
        "inner_dict": {"day_word_full": "Saturday"},
        "list": ["a", 123, "Sat"],
    }
    assert substitute_all_strftime_format_strings(input_dict) == expected_output_dict


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


@pytest.mark.parametrize(
    "url,expected",
    [
        ("s3://bucket/hello/world.csv.gz", "gzip"),
        ("s3://bucket/hello/world.csv", None),
        ("s3://bucket/hello/world.csv.xz", "xz"),
        ("s3://bucket/hello/world", None),
    ],
)
def test_sniff_s3_compression(url, expected):
    assert sniff_s3_compression(S3Url(url)) == expected


def test_azure_url():
    url = AzureUrl("my_account.blob.core.windows.net/my_container/my_blob")
    assert url.account_name == "my_account"
    assert url.container == "my_container"
    assert url.blob == "my_blob"


def test_azure_url_with_https():
    url = AzureUrl("https://my_account.blob.core.windows.net/my_container/my_blob")
    assert url.account_name == "my_account"
    assert url.container == "my_container"
    assert url.blob == "my_blob"


def test_azure_url_with_nested_blob():
    url = AzureUrl("my_account.blob.core.windows.net/my_container/a/b/c/d/e/my_blob")
    assert url.account_name == "my_account"
    assert url.container == "my_container"
    assert url.blob == "a/b/c/d/e/my_blob"
