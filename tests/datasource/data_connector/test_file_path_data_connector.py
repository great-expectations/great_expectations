from great_expectations.datasource.data_connector.file_path_data_connector import (
    FilePathDataConnector,
)


def test_sanitize_prefix_with_properly_formatted_dirname_input():
    prefix = "foo/"
    res = FilePathDataConnector.sanitize_prefix(prefix)
    assert res == "foo/"  # Unchanged due to already being formatted properly


def test_sanitize_prefix_with_dirname_input():
    prefix = "bar"
    res = FilePathDataConnector.sanitize_prefix(prefix)
    assert res == "bar/"


def test_sanitize_prefix_with_filename_input():
    prefix = "baz.txt"
    res = FilePathDataConnector.sanitize_prefix(prefix)
    assert res == "baz.txt"  # Unchanged due to already being formatted properly


def test_sanitize_prefix_with_nested_filename_input():
    prefix = "a/b/c/baz.txt"
    res = FilePathDataConnector.sanitize_prefix(prefix)
    assert res == "a/b/c/baz.txt"  # Unchanged due to already being formatted properly
