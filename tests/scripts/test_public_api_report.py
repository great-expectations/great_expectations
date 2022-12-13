import pathlib

import pytest


@pytest.fixture
def python_file_as_str() -> str:
    """A python file represented as a string, used in parsing tests."""

    # TODO: Replace with canned string
    filepath = "how_to_configure_a_configuredassetdataconnector.py"

    repo_root = pathlib.Path(__file__).parent.parent.parent
    assert str(repo_root).endswith("great_expectations")

    test_dir = repo_root / "tests" / ""

    with open(repo_root / "tests" / filepath) as f:
        file_contents: str = f.read()

    return file_contents


@pytest.fixture
def sample_python_file_string():
    return """import inspect

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import BaseDataContext

from great_expectations.expectations.expectation import ExpectationConfiguration

convert_to_json_serializable(1)

a = convert_to_json_serializable(2)
print(a)

ec = ExpectationConfiguration(expectation_type="expect_column_values_to_be_null", kwargs={})

b = ec.to_json_dict()
"""

def test_parse_method_names(python_file_as_str):
    """Ensure method names are retrieved from test file."""
    print(python_file_as_str)
