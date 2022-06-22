from typing import Any, Callable

import pytest

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    ConcurrencyConfig,
    DataContextConfig,
    NotebookConfig,
    NotebookTemplateConfig,
    ProgressBarsConfig,
)
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
            usage_statistics_url="https://www.my_usage_stats_url/test",
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
    "crud_method,target_attr",
    [
        pytest.param(
            "get_config_version",
            DataContextVariableSchema.CONFIG_VERSION,
            id="config_version getter",
        ),
        pytest.param(
            "get_config_variables_file_path",
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            id="config_variables_file_path getter",
        ),
        pytest.param(
            "get_plugins_directory",
            DataContextVariableSchema.PLUGINS_DIRECTORY,
            id="plugins_directory getter",
        ),
        pytest.param(
            "get_expectations_store_name",
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME,
            id="expectations_store getter",
        ),
        pytest.param(
            "get_validations_store_name",
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
            id="validations_store getter",
        ),
        pytest.param(
            "get_evaluation_parameter_store_name",
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            id="evaluation_parameter_store getter",
        ),
        pytest.param(
            "get_checkpoint_store_name",
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store getter",
        ),
        pytest.param(
            "get_profiler_store_name",
            DataContextVariableSchema.PROFILER_STORE_NAME,
            id="profiler_store getter",
        ),
        pytest.param(
            "get_stores", DataContextVariableSchema.STORES, id="stores getter"
        ),
        pytest.param(
            "get_data_docs_sites",
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites getter",
        ),
        pytest.param(
            "get_anonymous_usage_statistics",
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            id="anonymous_usage_statistics getter",
        ),
        pytest.param(
            "get_notebooks",
            DataContextVariableSchema.NOTEBOOKS,
            id="notebooks getter",
        ),
        pytest.param(
            "get_concurrency",
            DataContextVariableSchema.CONCURRENCY,
            id="concurrency getter",
        ),
        pytest.param(
            "get_progress_bars",
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
    crud_method: str,
    target_attr: DataContextVariableSchema,
) -> None:
    def _test_variables_get(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        res: Any = method()

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
    assert variables.get_config_version() == value_associated_with_env_var


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
            "set_config_variables_file_path",
            "uncommitted/my_config_file.yml",
            DataContextVariableSchema.CONFIG_VARIABLES_FILE_PATH,
            id="config_variables_file_path setter",
        ),
        pytest.param(
            "set_plugins_directory",
            "other_plugins/",
            DataContextVariableSchema.PLUGINS_DIRECTORY,
            id="plugins_directory setter",
        ),
        pytest.param(
            "set_expectations_store_name",
            "my_expectations_store",
            DataContextVariableSchema.EXPECTATIONS_STORE_NAME,
            id="expectations_store setter",
        ),
        pytest.param(
            "set_validations_store_name",
            "my_validations_store",
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
            id="validations_store setter",
        ),
        pytest.param(
            "set_evaluation_parameter_store_name",
            "my_evaluation_parameter_store",
            DataContextVariableSchema.EVALUATION_PARAMETER_STORE_NAME,
            id="evaluation_parameter_store setter",
        ),
        pytest.param(
            "set_checkpoint_store_name",
            "my_checkpoint_store",
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store setter",
        ),
        pytest.param(
            "set_profiler_store_name",
            "my_profiler_store",
            DataContextVariableSchema.PROFILER_STORE_NAME,
            id="profiler_store setter",
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
        pytest.param(
            "set_anonymous_usage_statistics",
            anonymous_usage_statistics,
            DataContextVariableSchema.ANONYMOUS_USAGE_STATISTICS,
            id="anonymous_usage_statistics setter",
        ),
        pytest.param(
            "set_notebooks",
            notebooks,
            DataContextVariableSchema.NOTEBOOKS,
            id="notebooks setter",
        ),
        pytest.param(
            "set_concurrency",
            concurrency,
            DataContextVariableSchema.CONCURRENCY,
            id="concurrency setter",
        ),
        pytest.param(
            "set_progress_bars",
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
    crud_method: str,
    input_value: Any,
    target_attr: DataContextVariableSchema,
) -> None:
    def _test_variables_set(type_: DataContextVariables) -> None:
        method: Callable = getattr(type_, crud_method)
        method(input_value)
        res: Any = type_.config[target_attr.value]

        assert res == input_value

    # EphemeralDataContextVariables
    _test_variables_set(ephemeral_data_context_variables)

    # FileDataContextVariables
    _test_variables_set(file_data_context_variables)

    # CloudDataContextVariables
    _test_variables_set(cloud_data_context_variables)
