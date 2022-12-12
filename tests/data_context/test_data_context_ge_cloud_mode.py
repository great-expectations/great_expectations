from typing import Callable
from unittest import mock

import pytest

from great_expectations.data_context import BaseDataContext, DataContext
from great_expectations.data_context.cloud_constants import CLOUD_DEFAULT_BASE_URL
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions import DataContextError, GXCloudError
from great_expectations.util import get_context


@pytest.mark.cloud
def test_data_context_ge_cloud_mode_with_incomplete_cloud_config_should_throw_error():
    # Don't want to make a real request in a unit test so we simply patch the config fixture
    with mock.patch(
        "great_expectations.data_context.CloudDataContext._get_cloud_config_dict",
        return_value={"base_url": None, "organization_id": None, "access_token": None},
    ):
        with pytest.raises(DataContextError):
            DataContext(context_root_dir="/my/context/root/dir", cloud_mode=True)


@pytest.mark.cloud
@mock.patch("requests.get")
def test_data_context_ge_cloud_mode_makes_successful_request_to_cloud_api(
    mock_request,
    request_headers: dict,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_access_token,
):
    # Ensure that the request goes through
    mock_request.return_value.status_code = 200
    try:
        DataContext(
            cloud_mode=True,
            cloud_base_url=ge_cloud_runtime_base_url,
            cloud_organization_id=ge_cloud_runtime_organization_id,
            cloud_access_token=ge_cloud_access_token,
        )
    except:  # Not concerned with constructor output (only evaluating interaction with requests during __init__)
        pass

    called_with_url = f"{ge_cloud_runtime_base_url}/organizations/{ge_cloud_runtime_organization_id}/data-context-configuration"
    called_with_header = {"headers": request_headers}

    # Only ever called once with the endpoint URL and auth token as args
    mock_request.assert_called_once()
    assert mock_request.call_args[0][0] == called_with_url
    assert mock_request.call_args[1] == called_with_header


@pytest.mark.cloud
@mock.patch("requests.get")
def test_data_context_ge_cloud_mode_with_bad_request_to_cloud_api_should_throw_error(
    mock_request,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_access_token,
):
    # Ensure that the request fails
    mock_request.return_value.status_code = 401

    with pytest.raises(GXCloudError):
        DataContext(
            cloud_mode=True,
            cloud_base_url=ge_cloud_runtime_base_url,
            cloud_organization_id=ge_cloud_runtime_organization_id,
            cloud_access_token=ge_cloud_access_token,
        )


@pytest.mark.cloud
@pytest.mark.unit
@mock.patch("requests.get")
def test_data_context_in_cloud_mode_passes_base_url_to_store_backend(
    mock_request,
    ge_cloud_base_url,
    empty_base_data_context_in_cloud_mode_custom_base_url: BaseDataContext,
    ge_cloud_runtime_organization_id,
    ge_cloud_access_token,
):

    custom_base_url: str = "https://some_url.org"
    # Ensure that the request goes through
    mock_request.return_value.status_code = 200

    context: BaseDataContext = empty_base_data_context_in_cloud_mode_custom_base_url

    # Assertions that the context fixture is set up properly
    assert not context.ge_cloud_config.base_url == CLOUD_DEFAULT_BASE_URL
    assert not context.ge_cloud_config.base_url == ge_cloud_base_url
    assert (
        not context.ge_cloud_config.base_url == "https://app.test.greatexpectations.io"
    )

    # The DatasourceStore should not have the default base_url or commonly used test base urls
    assert (
        not context._datasource_store.store_backend.config["ge_cloud_base_url"]
        == CLOUD_DEFAULT_BASE_URL
    )
    assert (
        not context._datasource_store.store_backend.config["ge_cloud_base_url"]
        == ge_cloud_base_url
    )
    assert (
        not context._datasource_store.store_backend.config["ge_cloud_base_url"]
        == "https://app.test.greatexpectations.io"
    )

    # The DatasourceStore should have the custom base url set
    assert (
        context._datasource_store.store_backend.config["ge_cloud_base_url"]
        == custom_base_url
    )
