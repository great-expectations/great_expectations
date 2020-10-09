import os

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path

BASE_DIR = "fixtures"


def test_DataContext_raises_error_on_config_not_found():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, ""))
    with pytest.raises(ge_exceptions.ConfigNotFoundError):
        DataContext(local_dir)


def test_DataContext_raises_error_on_unparsable_yaml_file():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, "bad_yml"))
    with pytest.raises(ge_exceptions.InvalidConfigurationYamlError):
        DataContext(local_dir)


# NOTE: 20191001 - JPC: The behavior of typed DataContextConfig is removed because it did not support
# round trip yaml comments. Re-add appropriate tests upon development of an appropriate replacement
# for DataContextConfig
# def test_DataContext_raises_error_on_invalid_top_level_key():
#     local_dir = file_relative_path(
#         __file__, os.path.join(BASE_DIR, "invalid_top_level_key")
#     )
#     with pytest.raises(ge_exceptions.InvalidTopLevelConfigKeyError):
#         DataContext(local_dir)
#
#
# def test_DataContext_raises_error_on_missing_top_level_key():
#     local_dir = file_relative_path(
#         __file__, os.path.join(BASE_DIR, "missing_top_level_key")
#     )
#     with pytest.raises(ge_exceptions.MissingTopLevelConfigKeyError):
#         DataContext(local_dir)


def test_DataContext_raises_error_on_invalid_top_level_type():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_top_level_value_type")
    )
    with pytest.raises(ge_exceptions.InvalidDataContextConfigError) as exc:
        DataContext(local_dir)

    assert "data_docs_sites" in exc.value.messages


def test_DataContext_raises_error_on_invalid_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_config_version")
    )
    with pytest.raises(ge_exceptions.InvalidDataContextConfigError) as exc:
        DataContext(local_dir)

    assert "config_version" in exc.value.messages


def test_DataContext_raises_error_on_old_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "old_config_version")
    )
    with pytest.raises(ge_exceptions.InvalidDataContextConfigError) as exc:
        DataContext(local_dir)

    assert "Error while processing DataContextConfig" in exc.value.message


def test_DataContext_raises_error_on_missing_config_version_aka_version_zero():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, "version_zero"))
    with pytest.raises(ge_exceptions.InvalidDataContextConfigError):
        DataContext(local_dir)
