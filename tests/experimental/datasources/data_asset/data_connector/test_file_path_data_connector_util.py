from great_expectations.experimental.datasources.data_asset.data_connector.file_path_data_connector_util import (
    sanitize_prefix,
)


def test_sanitize_prefix_with_properly_formatted_dirname_input():
    prefix = "foo/"
    res = sanitize_prefix(prefix)
    assert res == "foo/"  # Unchanged due to already being formatted properly


def test_sanitize_prefix_with_dirname_input():
    prefix = "bar"
    res = sanitize_prefix(prefix)
    assert res == "bar/"


def test_sanitize_prefix_with_filename_input():
    prefix = "baz.txt"
    res = sanitize_prefix(prefix)
    assert res == "baz.txt"  # Unchanged due to already being formatted properly


def test_sanitize_prefix_with_nested_filename_input():
    prefix = "a/b/c/baz.txt"
    res = sanitize_prefix(prefix)
    assert res == "a/b/c/baz.txt"  # Unchanged due to already being formatted properly
