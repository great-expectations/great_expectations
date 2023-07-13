import pathlib
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations.data_context import CloudDataContext, EphemeralDataContext
from great_expectations.data_context.cloud_constants import GXCloudEnvironmentVariable
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.exceptions.exceptions import GXCloudConfigurationError
from tests.test_utils import working_directory

GX_CLOUD_PARAMS_ALL = {
    "cloud_base_url": "http://hello.com",
    "cloud_organization_id": "bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
    "cloud_access_token": "i_am_a_token",
}
GX_CLOUD_PARAMS_REQUIRED = {
    "cloud_organization_id": "bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
    "cloud_access_token": "i_am_a_token",
}


@pytest.fixture()
def set_up_cloud_envs(monkeypatch):
    monkeypatch.setenv("GX_CLOUD_BASE_URL", "http://hello.com")
    monkeypatch.setenv(
        "GX_CLOUD_ORGANIZATION_ID", "bd20fead-2c31-4392-bcd1-f1e87ad5a79c"
    )
    monkeypatch.setenv("GX_CLOUD_ACCESS_TOKEN", "i_am_a_token")


@pytest.fixture
def clear_env_vars(monkeypatch):
    # Delete local env vars (if present)
    for env_var in GXCloudEnvironmentVariable:
        monkeypatch.delenv(env_var, raising=False)


@pytest.mark.unit
def test_base_context(clear_env_vars):
    config: DataContextConfig = DataContextConfig(
        config_version=3.0,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )
    assert isinstance(gx.get_context(project_config=config), EphemeralDataContext)


@pytest.mark.unit
def test_base_context__with_overridden_yml(tmp_path: pathlib.Path, clear_env_vars):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    gx.data_context.FileDataContext.create(project_path)
    context_path = project_path / "great_expectations"
    context = gx.get_context(context_root_dir=context_path)
    assert isinstance(context, FileDataContext)
    assert context.expectations_store_name == "expectations_store"

    config: DataContextConfig = DataContextConfig(
        config_version=3.0,
        plugins_directory=None,
        evaluation_parameter_store_name="new_evaluation_parameter_store",
        expectations_store_name="new_expectations_store",
        datasources={},
        stores={
            "new_expectations_store": {"class_name": "ExpectationsStore"},
            "new_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "new_validation_result_store": {"class_name": "ValidationsStore"},
        },
        validations_store_name="new_validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )
    context = gx.get_context(project_config=config, context_root_dir=context_path)
    assert isinstance(context, FileDataContext)
    assert context.expectations_store_name == "new_expectations_store"


@pytest.mark.unit
def test_data_context(tmp_path: pathlib.Path, clear_env_vars):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    gx.data_context.FileDataContext.create(project_path)
    with working_directory(project_path):
        assert isinstance(gx.get_context(), FileDataContext)


@pytest.mark.unit
def test_data_context_root_dir_returns_data_context(
    tmp_path: pathlib.Path,
    clear_env_vars,
):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    gx.data_context.FileDataContext.create(project_path)
    context_path = project_path / "great_expectations"
    assert isinstance(
        gx.get_context(context_root_dir=str(context_path)), FileDataContext
    )


@pytest.mark.unit
def test_base_context_invalid_root_dir(clear_env_vars, tmp_path):
    config: DataContextConfig = DataContextConfig(
        config_version=3.0,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )

    context_root_dir = tmp_path / "root"
    context_root_dir.mkdir()
    assert isinstance(
        gx.get_context(project_config=config, context_root_dir=context_root_dir),
        FileDataContext,
    )


@pytest.mark.parametrize("ge_cloud_mode", [True, None])
@pytest.mark.cloud
def test_cloud_context_env(
    set_up_cloud_envs, empty_ge_cloud_data_context_config, ge_cloud_mode
):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        assert isinstance(
            gx.get_context(cloud_mode=ge_cloud_mode),
            CloudDataContext,
        )


@pytest.mark.cloud
def test_cloud_context_disabled(set_up_cloud_envs, tmp_path: pathlib.Path):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    gx.data_context.FileDataContext.create(project_path)
    with working_directory(project_path):
        assert isinstance(gx.get_context(cloud_mode=False), FileDataContext)


@pytest.mark.cloud
def test_cloud_missing_env_throws_exception(
    clear_env_vars, empty_ge_cloud_data_context_config
):
    with pytest.raises(GXCloudConfigurationError):
        gx.get_context(cloud_mode=True),


@pytest.mark.parametrize("params", [GX_CLOUD_PARAMS_REQUIRED, GX_CLOUD_PARAMS_ALL])
@pytest.mark.cloud
def test_cloud_context_params(monkeypatch, empty_ge_cloud_data_context_config, params):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        assert isinstance(
            gx.get_context(**params),
            CloudDataContext,
        )


@pytest.mark.cloud
def test_cloud_context_with_in_memory_config_overrides(
    monkeypatch, empty_ge_cloud_data_context_config
):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        context = gx.get_context(
            cloud_base_url="http://hello.com",
            cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
            cloud_access_token="i_am_a_token",
        )
        assert isinstance(context, CloudDataContext)
        assert context.expectations_store_name == "default_expectations_store"
        assert context.variables.include_rendered_content.globally

        config: DataContextConfig = DataContextConfig(
            config_version=3.0,
            plugins_directory=None,
            evaluation_parameter_store_name="new_evaluation_parameter_store",
            expectations_store_name="new_expectations_store",
            datasources={},
            stores={
                "new_expectations_store": {"class_name": "ExpectationsStore"},
                "new_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "new_validation_result_store": {"class_name": "ValidationsStore"},
            },
            validations_store_name="new_validation_result_store",
            data_docs_sites={},
            validation_operators={},
        )
        context = gx.get_context(
            project_config=config,
            cloud_base_url="http://hello.com",
            cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
            cloud_access_token="i_am_a_token",
        )
        assert isinstance(context, CloudDataContext)
        assert context.expectations_store_name == "new_expectations_store"


@pytest.mark.unit
def test_get_context_with_no_arguments_returns_ephemeral_with_sensible_defaults():
    context = gx.get_context()
    assert isinstance(context, EphemeralDataContext)

    defaults = InMemoryStoreBackendDefaults(init_temp_docs_sites=True)
    assert context.config.stores == defaults.stores


@pytest.mark.parametrize("ge_cloud_mode", [True, None])
@pytest.mark.cloud
def test_cloud_context_include_rendered_content(
    set_up_cloud_envs, empty_ge_cloud_data_context_config, ge_cloud_mode
):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        context = gx.get_context(cloud_mode=ge_cloud_mode)
        assert isinstance(
            context,
            CloudDataContext,
        )
        assert context.variables.include_rendered_content.globally


@pytest.mark.filesystem
@pytest.mark.integration
def test_get_context_with_context_root_dir_scaffolds_filesystem(tmp_path: pathlib.Path):
    root = tmp_path / "root"
    context_root_dir = root.joinpath("great_expectations")
    assert not context_root_dir.exists()

    context = gx.get_context(context_root_dir=context_root_dir)

    assert isinstance(context, FileDataContext)
    assert context_root_dir.exists()
