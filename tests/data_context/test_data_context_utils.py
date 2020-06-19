import pytest

import great_expectations.exceptions as gee
from great_expectations.util import load_class


def test_load_class_raises_error_when_module_not_found():
    with pytest.raises(gee.PluginModuleNotFoundError):
        load_class("foo", "bar")


def test_load_class_raises_error_when_class_not_found():
    with pytest.raises(gee.PluginClassNotFoundError):
        load_class("TotallyNotARealClass", "great_expectations.datasource")
