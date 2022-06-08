from typing import Any, Callable
from unittest import mock

import pytest

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
    DataContextVariables,
    DataContextVariableSchema,
    EphemeralDataContextVariables,
    FileDataContextVariables,
)


@pytest.fixture
def data_context_config_dict() -> dict:
    config: dict = {
        "config_version": 2.0,
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "validations_store_name": "does_not_have_to_be_real",
        "expectations_store_name": "expectations_store",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "evaluation_parameter_store": {
                "module_name": "great_expectations.data_context.store",
                "class_name": "EvaluationParameterStore",
            },
        },
        "data_docs_sites": {},
        "anonymous_usage_statistics": {
            "enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
            "usage_statistics_url": "https://www.my_usage_stats_url/test",
        },
    }
    return config


@pytest.fixture
def ephemeral_data_context_variables(
    data_context_config_dict: dict,
) -> EphemeralDataContextVariables:
    return EphemeralDataContextVariables(**data_context_config_dict)


@pytest.fixture
def file_data_context_variables(
    data_context_config_dict: dict, empty_data_context: DataContext
) -> FileDataContextVariables:
    return FileDataContextVariables(
        data_context=empty_data_context, **data_context_config_dict
    )


@pytest.fixture
def cloud_data_context_variables(
    data_context_config_dict: dict,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> CloudDataContextVariables:
    return CloudDataContextVariables(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
        **data_context_config_dict,
    )


@pytest.fixture
def stores() -> dict:
    return {
        "profiler_store": {
            "class_name": "ProfilerStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "profilers/",
            },
        },
    }


@pytest.fixture
def data_docs_sites() -> dict:
    return {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/local_site/",
            },
        },
    }


@pytest.mark.parametrize(
    "crud_method,target_attr",
    [
        pytest.param(
            "get_config_version",
            DataContextVariableSchema.CONFIG_VERSION,
            id="config_version getter",
        ),
        pytest.param(
            "get_stores", DataContextVariableSchema.STORES, id="stores getter"
        ),
        pytest.param(
            "get_data_docs_sites",
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites getter",
        ),
    ],
)
def test_data_context_variables_get(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    data_context_config_dict: dict,
    crud_method: str,
    target_attr: DataContextVariableSchema,
) -> None:
    def _test_variables_get(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        res: Any = method()

        expected_value: Any = data_context_config_dict[target_attr.value]
        assert res == expected_value

    # EphemeralDataContextVariables
    _test_variables_get(ephemeral_data_context_variables)

    # FileDataContextVariables
    _test_variables_get(file_data_context_variables)

    # CloudDataContextVariables
    _test_variables_get(cloud_data_context_variables)


@pytest.mark.parametrize(
    "crud_method,input_value,target_attr",
    [
        pytest.param(
            "set_config_version",
            5.0,
            DataContextVariableSchema.CONFIG_VERSION,
            id="config_version setter",
        ),
        pytest.param(
            "set_stores", stores, DataContextVariableSchema.STORES, id="stores setter"
        ),
        pytest.param(
            "set_data_docs_sites",
            data_docs_sites,
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites setter",
        ),
    ],
)
def test_data_context_variables_set(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    crud_method: str,
    input_value: Any,
    target_attr: DataContextVariableSchema,
    # The below GE Cloud variables were used to instantiate the above CloudDataContextVariables
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> None:
    def _test_variables_set(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        method(input_value)
        res: Any = getattr(type_, target_attr.value)

        assert res == input_value

    # EphemeralDataContextVariables
    _test_variables_set(ephemeral_data_context_variables)

    # FileDataContextVariables
    with mock.patch(
        "great_expectations.data_context.DataContext._save_project_config",
        autospec=True,
    ) as mock_save:
        _test_variables_set(file_data_context_variables)

        assert mock_save.call_count == 1

    # CloudDataContextVariables
    with mock.patch("requests.post", autospec=True) as mock_post:
        _test_variables_set(cloud_data_context_variables)

        assert mock_post.call_count == 1
        mock_post.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/data-context-variables",
            json={
                "data": {
                    "type": "data_context_variable",
                    "attributes": {
                        "organization_id": ge_cloud_organization_id,
                        "value": input_value,
                        "variable_type": target_attr.value,
                    },
                }
            },
            headers={
                "Content-Type": "application/vnd.api+json",
                "Authorization": f"Bearer {ge_cloud_access_token}",
            },
        )
