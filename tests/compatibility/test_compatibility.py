
import pytest

from great_expectations.compatibility.google import google
from great_expectations.compatibility.docstring_parser import docstring_parser
from great_expectations.compatibility.azure import azure
from great_expectations.compatibility.not_imported import NotImported
from great_expectations.compatibility.pyspark import pyspark
from great_expectations.compatibility.aws import boto3
from great_expectations.compatibility.aws import botocore

# TODO: parametrize test_error_raised_when_optional_import_not_installed over all optional dependencies
# TODO: Test to make sure we don't have optional dependencies that are not in our set of tests in test_error_raised_when_optional_import_not_installed

libraries = [pyspark, boto3, botocore, azure, docstring_parser, google]

@pytest.mark.unit
@pytest.mark.parametrize(
    ["package"],
    [
        pytest.param(
            library,
        )
        for library in libraries
    ],
)
@pytest.mark.compatibility
def test_error_raised_when_optional_import_not_installed(package):

    assert isinstance(package, NotImported)
    assert not package
    with pytest.raises(ModuleNotFoundError):
        _ = package.some_param
