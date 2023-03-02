from types import ModuleType

import pytest
from great_expectations.util import maybe_import_module, maybe_import_attr


def test_maybe_import_installed_module():
    module = maybe_import_module("json")
    assert isinstance(module, ModuleType)


def test_maybe_import_uninstalled_module():
    module = maybe_import_module("this_module_does_not_exist")
    with pytest.raises(ModuleNotFoundError):
        module.something
    assert "please install" in module.__dict__["gx_error_message"].lower()


def test_maybe_import_existing_value_attr():
    attr = maybe_import_attr("datetime", "datetime")
    # This should not error
    attr.now()


def test_maybe_import_nonexisting_attr():
    with pytest.raises(AttributeError):
        attr = maybe_import_attr("datetime", "not_a_real_attribute")


def test_maybe_import_uninstalled_attr():
    attr = maybe_import_attr("this_module_does_not_exist", "an_attr")
    with pytest.raises(ModuleNotFoundError):
        attr()
