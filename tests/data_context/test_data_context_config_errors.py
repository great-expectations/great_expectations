import pytest
import os
from great_expectations import DataContext
from great_expectations.cli.init import file_relative_path
import great_expectations.exceptions as ge_exceptions

BASE_DIR = "fixtures"


def test_DataContext_raises_error_on_config_not_found():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, ""))
    with pytest.raises(ge_exceptions.ConfigNotFoundError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_unparsable_yaml_file():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, "bad_yml"))
    with pytest.raises(ge_exceptions.InvalidConfigurationYamlError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_invalid_top_level_key():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_top_level_key")
    )
    with pytest.raises(ge_exceptions.InvalidTopLevelConfigKeyError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_missing_top_level_key():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "missing_top_level_key")
    )
    with pytest.raises(ge_exceptions.MissingTopLevelConfigKeyError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_invalid_top_level_type():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_top_level_value_type")
    )
    with pytest.raises(ge_exceptions.InvalidConfigValueTypeError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_invalid_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_config_version")
    )
    with pytest.raises(ge_exceptions.InvalidConfigVersionError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_old_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "old_config_version")
    )
    with pytest.raises(ge_exceptions.UnsupportedConfigVersionError):
        DataContext(local_dir)
