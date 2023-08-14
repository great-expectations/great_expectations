from __future__ import annotations

import pathlib
import shutil
from typing import Callable

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import get_context

pytestmark = pytest.mark.filesystem

BASE_DIR = pathlib.Path(__file__).parent.joinpath("fixtures")


@pytest.fixture
def build_local_dir(tmp_path: pathlib.Path) -> Callable:
    def _build_local_dir(fixture_subdir: str | None = None) -> pathlib.Path:
        local_dir = tmp_path / "root"
        if fixture_subdir:
            fixture_path = file_relative_path(
                __file__, str(pathlib.Path(BASE_DIR) / fixture_subdir)
            )
            shutil.copytree(fixture_path, local_dir)
        return local_dir

    return _build_local_dir


def test_DataContext_raises_error_on_config_not_found(build_local_dir: Callable):
    local_dir = build_local_dir()
    with pytest.raises(gx_exceptions.ConfigNotFoundError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_unparsable_yaml_file(build_local_dir: Callable):
    local_dir = build_local_dir("bad_yml")
    with pytest.raises(gx_exceptions.InvalidConfigurationYamlError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_invalid_top_level_type(build_local_dir: Callable):
    local_dir = build_local_dir("invalid_top_level_value_type")
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    error_messages = []
    if exc.value.messages and len(exc.value.messages) > 0:
        error_messages.extend(exc.value.messages)
    if exc.value.message:
        error_messages.append(exc.value.message)
    error_messages = " ".join(error_messages)
    assert "data_docs_sites" in error_messages


def test_DataContext_raises_error_on_invalid_config_version(build_local_dir: Callable):
    local_dir = build_local_dir("invalid_config_version")
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    error_messages = []
    if exc.value.messages and len(exc.value.messages) > 0:
        error_messages.extend(exc.value.messages)
    if exc.value.message:
        error_messages.append(exc.value.message)
    error_messages = " ".join(error_messages)
    assert "config_version" in error_messages


def test_DataContext_raises_error_on_old_config_version(build_local_dir: Callable):
    local_dir = build_local_dir("old_config_version")
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError) as exc:
        get_context(context_root_dir=local_dir)

    assert "Error while processing DataContextConfig" in exc.value.message


def test_DataContext_raises_error_on_missing_config_version_aka_version_zero(
    build_local_dir: Callable,
):
    local_dir = build_local_dir("version_zero")
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError):
        get_context(context_root_dir=local_dir)


def test_DataContext_raises_error_on_missing_config_version_aka_version_zero_with_v2_config(
    build_local_dir: Callable,
):
    local_dir = build_local_dir("version_2-0_but_no_version_defined")
    with pytest.raises(gx_exceptions.InvalidDataContextConfigError):
        get_context(context_root_dir=local_dir)
