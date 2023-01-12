import os

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import get_context

BASE_DIR = "fixtures"


def test_DataContext_raises_error_on_config_not_found():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, ""))
    with pytest.raises(gx_exceptions.ConfigNotFoundError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_unparsable_yaml_file():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, "bad_yml"))
    with pytest.raises(gx_exceptions.InvalidConfigurationYamlError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_invalid_top_level_type():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_top_level_value_type")
    )
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    error_messages = []
    if exc.value.messages and len(exc.value.messages) > 0:
        error_messages.extend(exc.value.messages)
    if exc.value.message:
        error_messages.append(exc.value.message)
    error_messages = " ".join(error_messages)
    assert "data_docs_sites" in error_messages


def test_DataContext_raises_error_on_invalid_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "invalid_config_version")
    )
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    error_messages = []
    if exc.value.messages and len(exc.value.messages) > 0:
        error_messages.extend(exc.value.messages)
    if exc.value.message:
        error_messages.append(exc.value.message)
    error_messages = " ".join(error_messages)
    assert "config_version" in error_messages


def test_DataContext_raises_error_on_old_config_version():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "old_config_version")
    )
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    assert "Error while processing DataContextConfig" in exc.value.message


def test_DataContext_raises_error_on_missing_config_version_aka_version_zero():
    local_dir = file_relative_path(__file__, os.path.join(BASE_DIR, "version_zero"))
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_missing_config_version_aka_version_zero_with_v2_config():
    local_dir = file_relative_path(
        __file__, os.path.join(BASE_DIR, "version_2-0_but_no_version_defined")
    )
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError):
        get_context(context_root_dir=local_dir)
