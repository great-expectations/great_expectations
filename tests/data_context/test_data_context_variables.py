from __future__ import annotations

import copy
import pathlib
import urllib.parse
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY as MOCK_ANY

import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.config_provider import _ConfigurationProvider
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.context_factory import project_manager
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
    DataContextConfig,
    ProgressBarsConfig,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from great_expectations.data_context.types.resource_identifiers import (
        DataContextKey,
    )

yaml = YAMLHandler()


@pytest.fixture
def data_context_config_dict() -> dict:
    config: dict = {
        "config_version": 3.0,
        "plugins_directory": "plugins/",
        "validation_results_store_name": "validation_results_store",
        "expectations_store_name": "expectations_store",
        "checkpoint_store_name": "checkpoint_store",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
        },
        "data_docs_sites": {},
        "analytics_enabled": True,
        "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        "progress_bars": None,
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

    @override
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
    data_context_config: DataContextConfig, empty_data_context: FileDataContext
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
    ge_cloud_workspace_id: str,
    ge_cloud_access_token: str,
) -> CloudDataContextVariables:
    return CloudDataContextVariables(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_organization_id=ge_cloud_organization_id,
        ge_cloud_workspace_id=ge_cloud_workspace_id,
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
    context_root_dir = project_path / FileDataContext.GX_DIR
    context = FileDataContext(project_config=data_context_config, context_root_dir=context_root_dir)
    project_manager.set_project(context)
    return context


def stores() -> dict:
    return {
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "checkpoints/",
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
def progress_bars() -> ProgressBarsConfig:
    return ProgressBarsConfig(
        globally=True,
    )


@pytest.mark.unit
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
            id="validation_results_store getter",
        ),
        pytest.param(
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store getter",
        ),
        pytest.param(DataContextVariableSchema.STORES, id="stores getter"),
        pytest.param(
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites getter",
        ),
        pytest.param(
            DataContextVariableSchema.PROGRESS_BARS,
            id="progress_bars getter",
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


@pytest.mark.unit
def test_data_context_variables_get_with_substitutions(
    data_context_config_dict: dict,
) -> None:
    env_var_name: str = "MY_CONFIG_VERSION"
    value_associated_with_env_var: float = 7.0

    data_context_config_dict[DataContextVariableSchema.CONFIG_VERSION] = f"${env_var_name}"
    config: DataContextConfig = DataContextConfig(**data_context_config_dict)
    config_values: dict = {
        env_var_name: value_associated_with_env_var,
    }
    variables: DataContextVariables = EphemeralDataContextVariables(
        config=config,
        config_provider=StubConfigurationProvider(config_values=config_values),
    )
    assert variables.config_version == value_associated_with_env_var


@pytest.mark.unit
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
            "my_validation_results_store",
            DataContextVariableSchema.VALIDATIONS_STORE_NAME,
            id="validation_results_store setter",
        ),
        pytest.param(
            "my_checkpoint_store",
            DataContextVariableSchema.CHECKPOINT_STORE_NAME,
            id="checkpoint_store setter",
        ),
        pytest.param(stores, DataContextVariableSchema.STORES, id="stores setter"),
        pytest.param(
            data_docs_sites,
            DataContextVariableSchema.DATA_DOCS_SITES,
            id="data_docs_sites setter",
        ),
        pytest.param(
            progress_bars,
            DataContextVariableSchema.PROGRESS_BARS,
            id="progress_bars setter",
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


@pytest.mark.unit
def test_data_context_variables_save(
    mocker: MockerFixture,
    data_context_config_dict: dict,
    ephemeral_data_context_variables: EphemeralDataContextVariables,
    file_data_context_variables: FileDataContextVariables,
    cloud_data_context_variables: CloudDataContextVariables,
    # The below GX Cloud variables were used to instantiate the above CloudDataContextVariables
    v1_cloud_base_url: str,
    ge_cloud_organization_id: str,
    ge_cloud_workspace_id: str,
    ge_cloud_access_token: str,
) -> None:
    # EphemeralDataContextVariables
    ephemeral_data_context_variables.save()
    key: DataContextKey = ephemeral_data_context_variables.get_key()
    persisted_value = ephemeral_data_context_variables.store.get(key=key)
    assert isinstance(persisted_value, DataContextConfig)
    assert persisted_value.to_json_dict() == ephemeral_data_context_variables.config.to_json_dict()

    # FileDataContextVariables
    mock_save = mocker.patch(
        "great_expectations.data_context.store.InlineStoreBackend._save_changes",
        autospec=True,
    )
    file_data_context_variables.save()

    assert mock_save.call_count == 1

    # CloudDataContextVariables
    mock_put = mocker.patch("requests.Session.put", autospec=True)
    type(mock_put.return_value).status_code = mocker.PropertyMock(return_value=200)

    cloud_data_context_variables.save()

    expected_config_dict = {
        "analytics_enabled": True,
        "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "config_version": 3.0,
        "data_docs_sites": {},
        "plugins_directory": "plugins/",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "checkpoint_store": {"class_name": "CheckpointStore"},
            "validation_results_store": {"class_name": "ValidationResultsStore"},
            "validation_definition_store": {"class_name": "ValidationDefinitionStore"},
        },
    }

    assert mock_put.call_count == 1
    url = urllib.parse.urljoin(
        v1_cloud_base_url,
        f"organizations/{ge_cloud_organization_id}/workspaces/{ge_cloud_workspace_id}/data-context-variables",
    )
    mock_put.assert_called_with(
        MOCK_ANY,  # requests.Session object
        url,
        json={
            "data": expected_config_dict,
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


@pytest.mark.big
def test_file_data_context_variables_e2e(
    monkeypatch,
    file_data_context: FileDataContext,
    progress_bars: ProgressBarsConfig,
) -> None:
    """
    What does this test do and why?

    Tests the E2E workflow with a FileDataContextVariables instance.
      1. User updates certain values and sets them as attributes.
      2. User persists changes utilizing the save call defined by the Variables API.
      3. Upon reading the result config from disk, we can confirm that changes were appropriately persisted.

    It is also important to note that in the case of $VARS syntax, we NEVER want to persist the underlying
    value in order to preserve sensitive information.
    """  # noqa: E501 # FIXME CoP
    # Prepare updated progress_bars to set and serialize to disk
    updated_progress_bars: ProgressBarsConfig = copy.deepcopy(progress_bars)
    updated_progress_bars.globally = False

    # Prepare updated plugins directory to set and serialize to disk (ensuring we hide the true value behind $VARS syntax)  # noqa: E501 # FIXME CoP
    env_var_name: str = "MY_PLUGINS_DIRECTORY"
    value_associated_with_env_var: str = "foo/bar/baz"
    monkeypatch.setenv(env_var_name, value_associated_with_env_var)

    # Set attributes defined above
    file_data_context.variables.progress_bars = updated_progress_bars
    file_data_context.variables.plugins_directory = f"${env_var_name}"
    file_data_context.variables.save()

    # Review great_expectations.yml where values were written and confirm changes
    config_filepath = pathlib.Path(file_data_context.root_directory).joinpath(
        file_data_context.GX_YML
    )

    with open(config_filepath) as f:
        contents: dict = yaml.load(f)
        config_saved_to_disk: DataContextConfig = DataContextConfig(**contents)

    assert config_saved_to_disk.progress_bars == updated_progress_bars.to_dict()
    assert file_data_context.variables.plugins_directory == value_associated_with_env_var
    assert config_saved_to_disk.plugins_directory == f"${env_var_name}"
