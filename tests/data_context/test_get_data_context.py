"""
Why does this exist?

"""
import os
import pathlib
from typing import Any, Optional
from unittest import mock

import pytest
from typing_extensions import reveal_type

import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig


@pytest.mark.unit
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


# @pytest.mark.parametrize("project_config", "expected", [(DataContextConfig(
#         config_version=3.0,
#         plugins_directory=None,
#         evaluation_parameter_store_name="evaluation_parameter_store",
#         expectations_store_name="expectations_store",
#         datasources={},
#         stores={
#             "expectations_store": {"class_name": "ExpectationsStore"},
#             "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
#             "validation_result_store": {"class_name": "ValidationsStore"},
#         },
#         validations_store_name="validation_result_store",
#         data_docs_sites={},
#         validation_operators={},
#     ), EphemeralDataContext)])
# def test_get_context(
#     project_config: Any,
#     expected: Any
# ):
#     # is there a way to pass in a ClassName as Parameterized test?
#     assert isinstance(gx.get_context(project_config=project_config), expected)


def test_get_data_context_file(
    tmp_path: pathlib.Path,
):
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
