"""
Why does this exist?

"""
import os
from typing import List
from unittest import mock

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
    assert isinstance(gx.get_context(context_root_dir=context_path), FileDataContext)


@pytest.mark.cloud
@mock.patch("requests.get")
def test_DataContextOnly_cloud(
    mock_request,
    request_headers: dict,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_access_token,
):
    from great_expectations.data_context import CloudDataContext

    # Ensure that the request goes through
    mock_request.return_value.status_code = 200
    try:
        assert isinstance(
            gx.get_context(
                ge_cloud_base_url=ge_cloud_runtime_base_url,
                ge_cloud_access_token=ge_cloud_access_token,
                ge_cloud_organization_id=ge_cloud_runtime_organization_id,
            ),
            CloudDataContext,
        )
    except:  # Not concerned with constructor output (only evaluating interaction with requests during __init__)
        pass

    called_with_url = f"{ge_cloud_runtime_base_url}/organizations/{ge_cloud_runtime_organization_id}/data-context-configuration"
    called_with_header = {"headers": request_headers}

    # Only ever called once with the endpoint URL and auth token as args
    mock_request.assert_called_once()
    assert mock_request.call_args[0][0] == called_with_url
    assert mock_request.call_args[1] == called_with_header


def test_DataContextOnly_Ephemeral(basic_in_memory_data_context_config_just_stores):
    from great_expectations.data_context import EphemeralDataContext

    config: DataContextConfig = basic_in_memory_data_context_config_just_stores
    assert isinstance(gx.get_context(project_config=config), EphemeralDataContext)
