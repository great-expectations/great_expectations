import copy
import pathlib
import random
import string
from typing import Any
from unittest import mock

import pytest

from great_expectations.core.config_provider import _ConfigurationProvider
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
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
    GXCloudConfig,
    IncludeRenderedContentConfig,
    NotebookConfig,
    NotebookTemplateConfig,
    ProgressBarsConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)

yaml = YAMLHandler()


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
            usage_statistics_url=USAGE_STATISTICS_QA_URL,
        ),
        "notebooks": None,
        "concurrency": None,
        "progress_bars": None,
        "include_rendered_content": {
            "expectation_suite": False,
            "expectation_validation_result": False,
            "globally": False,
        },
    }
    return config


@pytest.fixture
def data_context_config(data_context_config_dict: dict) -> DataContextConfig:
    config: DataContextConfig = DataContextConfig(**data_context_config_dict)
    return config


class StubConfigurationProvider(_ConfigurationProvider):
    def __init__(self, config_values=None) -> None:
        self._config_values = config_values or {}
        super().__init__()

    def get_values(self):
        return self._config_values


@pytest.fixture
def ephemeral_data_context_variables(
    data_context_config: DataContextConfig,
) -> EphemeralDataContextVariables:
    return EphemeralDataContextVariables(
        config=data_context_config, config_provider=StubConfigurationProvider()
    )


@pytest.fixture
def file_data_context_variables(
    data_context_config: DataContextConfig, empty_data_context: DataContext
) -> FileDataContextVariables:
    return FileDataContextVariables(
        data_context=empty_data_context,
        config=data_context_config,
        config_provider=StubConfigurationProvider(),
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
        config_provider=StubConfigurationProvider(),
    )


@pytest.fixture
def file_data_context(
    tmp_path: pathlib.Path, data_context_config: DataContextConfig
) -> FileDataContext:
    project_path = tmp_path / "file_data_context"
    project_path.mkdir()
    context_root_dir = project_path / "great_expectations"
    context = FileDataContext(
        project_config=data_context_config, context_root_dir=str(context_root_dir)
    )
    return context


@pytest.fixture
def cloud_data_context(
    tmp_path: pathlib.Path,
    data_context_config: DataContextConfig,
    ge_cloud_config_e2e: GXCloudConfig,
) -> CloudDataContext:
    project_path = tmp_path / "cloud_data_context"
    project_path.mkdir()
    context_root_dir = project_path / "great_expectations"

    cloud_data_context = CloudDataContext(
        project_config=data_context_config,
        ge_cloud_base_url=ge_cloud_config_e2e.base_url,
        ge_cloud_access_token=ge_cloud_config_e2e.access_token,
        ge_cloud_organization_id=ge_cloud_config_e2e.organization_id,
        context_root_dir=str(context_root_dir),
    )
    return cloud_data_context


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


@pytest.fixture
def include_rendered_content() -> IncludeRenderedContentConfig:
    return IncludeRenderedContentConfig(
        globally=False,
        expectation_validation_result=False,
        expectation_suite=False,
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
        pytest.param(
            DataContextVariableSchema.INCLUDE_RENDERED_CONTENT,
            id="include_rendered_content getter",
        ),
    ],
)
@pytest.mark.slow  # 1.20s
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
    config_values: dict = {
        env_var_name: value_associated_with_env_var,
    }
    variables: DataContextVariables = EphemeralDataContextVariables(
        config=config,
        config_provider=StubConfigurationProvider(config_values=config_values),
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
        pytest.param(
            include_rendered_content,
            DataContextVariableSchema.INCLUDE_RENDERED_CONTENT,
            id="include_rendered_content setter",
        ),
    ],
)
@pytest.mark.slow  # 1.20s
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
    data_context_config_dict: dict,
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
        "great_expectations.data_context.store.InlineStoreBackend._save_changes",
        autospec=True,
    ) as mock_save:
        file_data_context_variables.save_config()

        assert mock_save.call_count == 1

    # CloudDataContextVariables
    with mock.patch("requests.Session.put", autospec=True) as mock_put:
        type(mock_put.return_value).status_code = mock.PropertyMock(return_value=200)

        cloud_data_context_variables.save_config()

        expected_config_dict: dict = {}
        for attr in (
            "config_variables_file_path",
            "config_version",
            "data_docs_sites",
            "notebooks",
            "plugins_directory",
            "stores",
            "include_rendered_content",
        ):
            expected_config_dict[attr] = data_context_config_dict[attr]

        assert mock_put.call_count == 1
        mock_put.assert_called_with(
            mock.ANY,  # requests.Session object
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
        )


@pytest.mark.unit
def test_data_context_variables_repr_and_str_only_reveal_config(
    data_context_config: DataContextConfig,
) -> None:
    config = data_context_config

    variables = EphemeralDataContextVariables(
        config=data_context_config,
        config_provider=StubConfigurationProvider(),
    )

    variables_str = str(variables)
    variables_repr = repr(variables)

    assert variables_str == str(config)
    assert variables_repr == repr(config)


@pytest.mark.integration
def test_file_data_context_variables_e2e(
    monkeypatch,
    file_data_context: FileDataContext,
    progress_bars: ProgressBarsConfig,
    include_rendered_content: IncludeRenderedContentConfig,
) -> None:
    """
    What does this test do and why?

    Tests the E2E workflow with a FileDataContextVariables instance.
      1. User updates certain values and sets them as attributes.
      2. User persists changes utilizing the save_config call defined by the Variables API.
      3. Upon reading the result config from disk, we can confirm that changes were appropriately persisted.

    It is also important to note that in the case of $VARS syntax, we NEVER want to persist the underlying
    value in order to preserve sensitive information.
    """
    # Prepare updated progress_bars to set and serialize to disk
    updated_progress_bars: ProgressBarsConfig = copy.deepcopy(progress_bars)
    updated_progress_bars.globally = False
    updated_progress_bars.profilers = True

    # Prepare updated include_rendered_content to set and serialize to disk
    updated_include_rendered_content: IncludeRenderedContentConfig = copy.deepcopy(
        include_rendered_content
    )
    updated_include_rendered_content.expectation_validation_result = True

    # Prepare updated plugins directory to set and serialize to disk (ensuring we hide the true value behind $VARS syntax)
    env_var_name: str = "MY_PLUGINS_DIRECTORY"
    value_associated_with_env_var: str = "foo/bar/baz"
    monkeypatch.setenv(env_var_name, value_associated_with_env_var)

    # Set attributes defined above
    file_data_context.variables.progress_bars = updated_progress_bars
    file_data_context.variables.include_rendered_content = (
        updated_include_rendered_content
    )
    file_data_context.variables.plugins_directory = f"${env_var_name}"
    file_data_context.variables.save_config()

    # Review great_expectations.yml where values were written and confirm changes
    config_filepath = pathlib.Path(file_data_context.root_directory).joinpath(
        file_data_context.GE_YML
    )

    with open(config_filepath) as f:
        contents: dict = yaml.load(f)
        config_saved_to_disk: DataContextConfig = DataContextConfig(**contents)

    assert config_saved_to_disk.progress_bars == updated_progress_bars.to_dict()
    assert (
        config_saved_to_disk.include_rendered_content.to_dict()
        == updated_include_rendered_content.to_dict()
    )
    assert (
        file_data_context.variables.plugins_directory == value_associated_with_env_var
    )
    assert config_saved_to_disk.plugins_directory == f"${env_var_name}"


@pytest.mark.e2e
@pytest.mark.cloud
def test_cloud_data_context_variables_successfully_hits_cloud_endpoint(
    cloud_data_context: CloudDataContext,
    data_context_config: DataContextConfig,
) -> None:
    """
    What does this test do and why?

    Ensures that the endpoint responsible for the DataContextVariables resource is accessible
    through the Variables API.
    """
    cloud_data_context.variables.config = data_context_config
    success = cloud_data_context.variables.save_config()

    assert success is True


@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
@pytest.mark.xfail(
    strict=False,
    reason="GX Cloud E2E tests are failing due to env vars not being consistently recognized by Docker; x-failing for purposes of 0.15.22 release",
)
def test_cloud_enabled_data_context_variables_e2e(
    mock_save_project_config: mock.MagicMock, data_docs_sites: dict, monkeypatch
) -> None:
    """
    What does this test do and why?

    Tests the E2E workflow with a Cloud-enabled DataContext; as the CloudDataContext does not yet have 1-to-1
    feature parity with the DataContext (as v0.15.15), this is the primary mechanism by which Great
    Expectations Cloud interacts with variables.
      1. User updates certain values and sets them as attributes.
      2. User persists changes utilizing the save_config call defined by the Variables API.
      3. Upon reading the result config from a GET request, we can confirm that changes were appropriately persisted.

    It is also important to note that in the case of $VARS syntax, we NEVER want to persist the underlying
    value in order to preserve sensitive information.
    """
    # Prepare updated plugins directory to set and save to the Cloud backend.
    # As values are persisted in the Cloud DB, we want to randomize our values each time for consistent test results
    updated_plugins_dir = f"plugins_dir_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"

    updated_data_docs_sites = data_docs_sites
    new_site_name = f"docs_site_{''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))}"
    updated_data_docs_sites[new_site_name] = {}

    context = DataContext(ge_cloud_mode=True)

    assert context.variables.plugins_directory != updated_plugins_dir
    assert context.variables.data_docs_sites != updated_data_docs_sites

    context.variables.plugins_directory = updated_plugins_dir
    context.variables.data_docs_sites = updated_data_docs_sites

    assert context.variables.plugins_directory == updated_plugins_dir
    assert context.variables.data_docs_sites == updated_data_docs_sites

    context.variables.save_config()

    context = DataContext(ge_cloud_mode=True)

    assert context.variables.plugins_directory == updated_plugins_dir
    assert context.variables.data_docs_sites == updated_data_docs_sites
