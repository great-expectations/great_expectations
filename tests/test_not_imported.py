import pytest

from great_expectations.compatibility.not_imported import NotImported


@pytest.fixture
def not_imported() -> NotImported:
    return NotImported("Please install foo")


@pytest.mark.unit
def test_get_attr_on_not_installed(not_imported):
    with pytest.raises(ModuleNotFoundError, match=str(not_imported)):
        _ = not_imported.thing


@pytest.mark.unit
def test_set_attr_on_not_installed(not_imported):
    with pytest.raises(ModuleNotFoundError, match=str(not_imported)):
        not_imported.thing = 5


@pytest.mark.unit
def test_calling_not_installed(not_imported):
    with pytest.raises(ModuleNotFoundError, match=str(not_imported)):
        not_imported()
