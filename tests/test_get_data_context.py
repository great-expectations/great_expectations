"""
Why does this exist?

"""
import os
from typing import List

import pytest

# from https://jmcgeheeiv.github.io/pyfakefs/release/usage.html
from pyfakefs.fake_filesystem_unittest import TestCase

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.exceptions import ConfigNotFoundError


def test_DataContextOnly(tmp_path):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    gx.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    from great_expectations.data_context import DataContext

    os.chdir(project_path)
    assert isinstance(gx.get_context(), DataContext)


def test_DataContextOnly_file(tmp_path):
    # anonymous usage statistics
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    gx.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    os.makedirs(asset_config_path, exist_ok=True)
    from great_expectations.data_context import FileDataContext

    os.chdir(project_path)
    assert isinstance(gx.get_context(context_root_dir=project_path), FileDataContext)
