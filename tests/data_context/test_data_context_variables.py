from typing import Any, Callable, cast
from unittest import mock

import pytest

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.data_context_variables import (
    CloudDataContextVariables,
    DataContextVariables,
    DataContextVariableSchema,
    EphemeralDataContextVariables,
    FileDataContextVariables,
)
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    ConcurrencyConfig,
    DataContextConfig,
    NotebookConfig,
    NotebookTemplateConfig,
    ProgressBarsConfig,
    dataContextConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)


@pytest.fixture
def data_context_config_dict() -> dict:
    config: dict = {
        "config_version": 3.0,
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "validations_store_name": "validations_store",
        "expectations_store_name": "expectations_store",
        "checkpoint_store_name": "checkpoint_store",
        "profiler_store_name": "profiler_store",
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
        "anonymous_usage_statistics": AnonymizedUsageStatisticsConfig(
            enabled=True,
            data_context_id="6a52bdfa-e182-455b-a825-e69f076e67d6",
            usage_statistics_url="https://app.greatexpectations.io/",
        ),
        "notebooks": None,
        "concurrency": None,
        "progress_bars": None,
    }
    return config


@pytest.fixture
def data_context_config(data_context_config_dict: dict) -> DataContextConfig:
    config: DataContextConfig = DataContextConfig(**data_context_config_dict)
    return config


@pytest.fixture
def ephemeral_data_context_variables(
    data_context_config: DataContextConfig,
) -> EphemeralDataContextVariables:
    return EphemeralDataContextVariables(config=data_context_config)


@pytest.fixture
def file_data_context_variables(
    data_context_config: DataContextConfig, empty_data_context: DataContext
) -> FileDataContextVariables:
    return FileDataContextVariables(
        data_context=empty_data_context, config=data_context_config
    )


@pytest.fixture
def cloud_data_context_variables(
    data_context_config: DataContextConfig,
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> CloudDataContextVariables:
    return CloudDataContextVariables(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_access_token=ge_cloud_access_token,
        config=data_context_config,
    )


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
        }
    }


@pytest.fixture
def anonymous_usage_statistics() -> AnonymizedUsageStatisticsConfig:
    return AnonymizedUsageStatisticsConfig(
        enabled=False,
    )


@pytest.fixture
def notebooks() -> NotebookConfig:
    return NotebookConfig(
        class_name="SuiteEditNotebookRenderer",
        module_name="great_expectations.render.renderer.v3.suite_edit_notebook_renderer",
        header_markdown=NotebookTemplateConfig(
            file_name="my_notebook_template.md",
        ),
    )


@pytest.fixture
def concurrency() -> ConcurrencyConfig:
    return ConcurrencyConfig(enabled=True)


@pytest.fixture
def progress_bars() -> ProgressBarsConfig:
    return ProgressBarsConfig(
        globally=True,
        profilers=False,
    )


@pytest.mark.parametrize(
    "target_attr",
    [
        pytest.param(
            DataContextVariableSchema.CONFIG_VERSION,
            id="config_version getter",
        ),
        pytest.param(
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            id="config_variables_file_path getter",
        ),
        pytest.param(
            DataContextVariableSchema.PLUGINS_DIRECTORY,
            id="plugins_directory getter",
        ),
        pytest.param(
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME,
            id="expectations_store getter",
        ),
        pytest.param(
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
            id="validations_store getter",
        ),
        pytest.param(
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            id="evaluation_parameter_store getter",
        ),
        pytest.param(
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store getter",
        ),
        pytest.param(
            DataContextVariableSchema.PROFILER_STORE_NAME,
            id="profiler_store getter",
        ),
        pytest.param(DataContextVariableSchema.STORES, id="stores getter"),
        pytest.param(
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites getter",
        ),
        pytest.param(
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            id="anonymous_usage_statistics getter",
        ),
        pytest.param(
            DataContextVariableSchema.NOTEBOOKS,
            id="notebooks getter",
        ),
        pytest.param(
            DataContextVariableSchema.CONCURRENCY,
            id="concurrency getter",
        ),
        pytest.param(
            DataContextVariableSchema.PROGRESS_BARS,
            id="progress_bars getter",
        ),
    ],
)
def test_data_context_variables_get(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    data_context_config: dict,
    target_attr: DataContextVariableSchema,
) -> None:
    def _test_variables_get(type_: DataContextVariables) -> None:
        res: Any = getattr(type_, target_attr.value)

        expected_value: Any = data_context_config[target_attr.value]
        assert res == expected_value

    # EphemeralDataContextVariables
    _test_variables_get(ephemeral_data_context_variables)

    # FileDataContextVariables
    _test_variables_get(file_data_context_variables)

    # CloudDataContextVariables
    _test_variables_get(cloud_data_context_variables)


def test_data_context_variables_get_with_substitutions(
    data_context_config_dict: dict,
) -> None:
    env_var_name: str = "MY_CONFIG_VERSION"
    value_associated_with_env_var: float = 7.0

    data_context_config_dict[
        DataContextVariableSchema.CONFIG_VERSION
    ] = f"${env_var_name}"
    config: DataContextConfig = DataContextConfig(**data_context_config_dict)
    substitutions: dict = {
        env_var_name: value_associated_with_env_var,
    }

    variables: DataContextVariables = EphemeralDataContextVariables(
        config=config, substitutions=substitutions
    )
    assert variables.config_version == value_associated_with_env_var


@pytest.mark.parametrize(
    "input_value,target_attr",
    [
        pytest.param(
            5.0,
            DataContextVariableSchema.CONFIG_VERSION,
            id="config_version setter",
        ),
        pytest.param(
            "uncommitted/my_config_file.yml",
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            id="config_variables_file_path setter",
        ),
        pytest.param(
            "other_plugins/",
            DataContextVariableSchema.PLUGINS_DIRECTORY,
            id="plugins_directory setter",
        ),
        pytest.param(
            "my_expectations_store",
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME,
            id="expectations_store setter",
        ),
        pytest.param(
            "my_validations_store",
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
            id="validations_store setter",
        ),
        pytest.param(
            "my_evaluation_parameter_store",
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            id="evaluation_parameter_store setter",
        ),
        pytest.param(
            "my_checkpoint_store",
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store setter",
        ),
        pytest.param(
            "my_profiler_store",
            DataContextVariableSchema.PROFILER_STORE_NAME,
            id="profiler_store setter",
        ),
        pytest.param(stores, DataContextVariableSchema.STORES, id="stores setter"),
        pytest.param(
            data_docs_sites,
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites setter",
        ),
        pytest.param(
            anonymous_usage_statistics,
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            id="anonymous_usage_statistics setter",
        ),
        pytest.param(
            notebooks,
            DataContextVariableSchema.NOTEBOOKS,
            id="notebooks setter",
        ),
        pytest.param(
            concurrency,
            DataContextVariableSchema.CONCURRENCY,
            id="concurrency setter",
        ),
        pytest.param(
            progress_bars,
            DataContextVariableSchema.PROGRESS_BARS,
            id="progress_bars setter",
        ),
    ],
)
def test_data_context_variables_set(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    input_value: Any,
    target_attr: DataContextVariableSchema,
) -> None:
    def _test_variables_set(type_: DataContextVariables) -> None:
        setattr(type_, target_attr.value, input_value)
        res: Any = type_.config[target_attr.value]

        assert res == input_value

    # EphemeralDataContextVariables
    _test_variables_set(ephemeral_data_context_variables)

    # FileDataContextVariables
    _test_variables_set(file_data_context_variables)

    # CloudDataContextVariables
    _test_variables_set(cloud_data_context_variables)


def test_data_context_variables_save_config(
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    # The below GE Cloud variables were used to instantiate the above CloudDataContextVariables
    ge_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_access_token: str,
) -> None:

    # EphemeralDataContextVariables
    ephemeral_data_context_variables.save_config()
    key: ConfigurationIdentifier = ephemeral_data_context_variables.get_key()
    persisted_value: DataContextConfig = ephemeral_data_context_variables.store.get(
        key=key
    )
    assert (
        persisted_value.to_json_dict()
        == ephemeral_data_context_variables.config.to_json_dict()
    )

    # FileDataContextVariables
    with mock.patch(
        "great_expectations.data_context.DataContext._save_project_config",
        autospec=True,
    ) as mock_save:
        file_data_context_variables.save_config()

        assert mock_save.call_count == 1

    # CloudDataContextVariables
    with mock.patch("requests.patch", autospec=True) as mock_patch:
        type(mock_patch.return_value).status_code = mock.PropertyMock(return_value=200)

        cloud_data_context_variables.save_config()

        expected_config_dict: dict = cast(
            dict, dataContextConfigSchema.dump(cloud_data_context_variables.config)
        )

        assert mock_patch.call_count == 1
        mock_patch.assert_called_with(
            f"{ge_cloud_base_url}/organizations/{ge_cloud_organization_id}/data-context-variables",
            json={
                "data": {
                    "type": "data_context_variables",
                    "attributes": {
                        "organization_id": ge_cloud_organization_id,
                        "data_context_variables": expected_config_dict,
                    },
                }
            },
            headers={
                "Content-Type": "application/vnd.api+json",
                "Authorization": f"Bearer {ge_cloud_access_token}",
            },
        )
