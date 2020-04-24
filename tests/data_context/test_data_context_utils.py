import pytest
import os

from great_expectations.data_context.util import (
    safe_mmkdir,
    load_class,
)
import great_expectations.exceptions as gee

def test_safe_mmkdir(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('empty_dir'))

    first_path = os.path.join(project_path,"first_path")

    safe_mmkdir(first_path)
    assert os.path.isdir(first_path)

    with pytest.raises(TypeError):
        safe_mmkdir(1)


def test_load_class_raises_error_when_module_not_found():
    with pytest.raises(gee.PluginModuleNotFoundError):
        load_class("foo", "bar")


def test_load_class_raises_error_when_class_not_found():
    with pytest.raises(gee.PluginClassNotFoundError):
        load_class("TotallyNotARealClass", "great_expectations.datasource")
