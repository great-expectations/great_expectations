import pytest

from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
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
    basic_data_context_config_dict: dict,
) -> FileDataContextVariables:
    return FileDataContextVariables(**basic_data_context_config_dict)


@pytest.fixture
def cloud_data_context_variables(
    basic_data_context_config_dict: dict,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
) -> CloudDataContextVariables:
    base_url: str = f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}"
    return CloudDataContextVariables(
        base_url=base_url, **basic_data_context_config_dict
    )


def test_ephemeral_data_context_variables_get(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
) -> None:
    res = ephemeral_data_context_variables.get_config_version()


def test_ephemeral_data_context_variables_set() -> None:
    pass


def test_file_data_context_variables_get() -> None:
    pass


def test_file_data_context_variables_set() -> None:
    pass


def test_cloud_data_context_variables_get() -> None:
    pass


def test_cloud_data_context_variables_set() -> None:
    pass
