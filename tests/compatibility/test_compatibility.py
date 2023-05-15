import pytest

from great_expectations.compatibility.not_imported import NotImported

# TODO: parametrize test_error_raised_when_optional_import_not_installed over all optional dependencies
# TODO: Test to make sure we don't have optional dependencies that are not in our set of tests in test_error_raised_when_optional_import_not_installed

@pytest.mark.compatibility
def test_error_raised_when_optional_import_not_installed():
    from great_expectations.compatibility.pyspark import pyspark
    breakpoint()
    assert isinstance(pyspark, NotImported)
    assert not pyspark
    with pytest.raises(ModuleNotFoundError):
        pyspark.some_param