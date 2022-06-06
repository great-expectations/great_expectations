from typing import Any, Callable
from unittest import mock

import pytest

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
    DataContextVariables,
    EphemeralDataContextVariables,
    FileDataContextVariables,
)


@pytest.fixture
def ephemeral_data_context_variables(
    basic_data_context_config_dict: dict,
) -> EphemeralDataContextVariables:
    return EphemeralDataContextVariables(**basic_data_context_config_dict)


@pytest.fixture
def file_data_context_variables(
    basic_data_context_config_dict: dict, empty_data_context: DataContext
) -> FileDataContextVariables:
    return FileDataContextVariables(
        data_context=empty_data_context, **basic_data_context_config_dict
    )


@pytest.fixture
def cloud_data_context_variables(
    basic_data_context_config_dict: dict,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> CloudDataContextVariables:
    base_url: str = f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}"
    return CloudDataContextVariables(
        ge_cloud_base_url=base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
        **basic_data_context_config_dict,
    )


@pytest.mark.parametrize(
    "crud_method,expected_value",
    [pytest.param("get_config_version", 2.0)],
)
def test_data_context_variables_get(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    crud_method: str,
    expected_value: Any,
) -> None:
    def _test_variables_get(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        res: Any = method()

        assert res == expected_value

    # EphemeralDataContextVariables
    _test_variables_get(ephemeral_data_context_variables)

    # FileDataContextVariables
    _test_variables_get(file_data_context_variables)

    # CloudDataContextVariables
    _test_variables_get(cloud_data_context_variables)


@pytest.mark.parametrize(
    "crud_method,input_value,target_attr",
    [pytest.param("set_config_version", 5.0, "config_version")],
)
def test_data_context_variables_set(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    crud_method: str,
    input_value: Any,
    target_attr: str,
) -> None:
    def _test_variables_set(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        method(input_value)
        res: Any = getattr(type_, target_attr)

        assert res == input_value

    # EphemeralDataContextVariables
    _test_variables_set(ephemeral_data_context_variables)

    # FileDataContextVariables
    with mock.patch(
        "great_expectations.data_context.DataContext._save_project_config"
    ) as mock_save:
        _test_variables_set(file_data_context_variables)
        assert mock_save.call_count == 1

    # CloudDataContextVariables
    with mock.patch("requests.post") as mock_post:
        _test_variables_set(cloud_data_context_variables)
        assert mock_post.call_count == 1
