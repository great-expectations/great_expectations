import pathlib
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext, CloudDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions import ConfigNotFoundError
from tests.test_utils import working_directory


@pytest.mark.unit
def test_config_returns_base_context():
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
    assert isinstance(gx.get_context(project_config=config), BaseDataContext)


@pytest.mark.unit
def test_empty_call_returns_data_context(tmp_path: pathlib.Path):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path_str = str(project_path)
    gx.data_context.DataContext.create(project_path_str)
    with working_directory(project_path_str):
        assert isinstance(gx.get_context(), DataContext)


@pytest.mark.unit
def test_context_root_dir_returns_data_context(
    tmp_path: pathlib.Path,
):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path_str = str(project_path)
    gx.data_context.DataContext.create(project_path_str)
    context_path = project_path / "great_expectations"
    assert isinstance(gx.get_context(context_root_dir=str(context_path)), DataContext)


@pytest.mark.unit
def test_context_invalid_root_dir_gives_error():
    with pytest.raises(ConfigNotFoundError):
        gx.get_context(context_root_dir="i/dont/exist")


@pytest.mark.unit
def test_config_returns_base_context_invalid_root_dir():
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
    assert isinstance(
        gx.get_context(project_config=config, context_root_dir="i/dont/exist"),
        BaseDataContext,
    )


@pytest.mark.unit
def test_config_returns_base_context_overrides_yml(tmp_path: pathlib.Path):
    project_path = tmp_path / "empty_data_context"
    project_path.mkdir()
    project_path_str = str(project_path)
    gx.data_context.DataContext.create(project_path_str)
    context_path = project_path / "great_expectations"
    context = gx.get_context(context_root_dir=context_path)
    assert isinstance(context, DataContext)
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
    assert isinstance(context, BaseDataContext)
    assert context.expectations_store_name == "new_expectations_store"


@pytest.mark.cloud
def test_cloud_config_in_env_returns_cloud_context(
    monkeypatch, empty_ge_cloud_data_context_config
):
    monkeypatch.setenv("GE_CLOUD_BASE_URL", "http://hello.com")
    monkeypatch.setenv(
        "GE_CLOUD_ORGANIZATION_ID", "bd20fead-2c31-4392-bcd1-f1e87ad5a79c"
    )
    monkeypatch.setenv("GE_CLOUD_ACCESS_TOKEN", "i_am_a_token")
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_ge_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        assert isinstance(
            gx.get_context(),
            CloudDataContext,
        )


@pytest.mark.cloud
def test_most_cloud_config_in_param_base_url_in_env_returns_cloud_context(
    monkeypatch, empty_ge_cloud_data_context_config
):
    monkeypatch.setenv("GE_CLOUD_BASE_URL", "http://hello.com")
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_ge_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        assert isinstance(
            gx.get_context(
                ge_cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
                ge_cloud_access_token="i_am_a_token",
            ),
            CloudDataContext,
        )


@pytest.mark.cloud
def test_all_cloud_config_in_param_returns_cloud_context(
    monkeypatch, empty_ge_cloud_data_context_config
):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_ge_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        assert isinstance(
            gx.get_context(
                ge_cloud_base_url="http://hello.com",
                ge_cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
                ge_cloud_access_token="i_am_a_token",
            ),
            CloudDataContext,
        )


@pytest.mark.cloud
def test_all_cloud_config_in_param_returns_cloud_context_and_in_memory_config_overrides(
    monkeypatch, empty_ge_cloud_data_context_config
):
    with mock.patch.object(
        CloudDataContext,
        "retrieve_data_context_config_from_ge_cloud",
        return_value=empty_ge_cloud_data_context_config,
    ):
        context = gx.get_context(
            ge_cloud_base_url="http://hello.com",
            ge_cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
            ge_cloud_access_token="i_am_a_token",
        )
        assert isinstance(context, CloudDataContext)
        assert context.expectations_store_name == "default_expectations_store"

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
            ge_cloud_base_url="http://hello.com",
            ge_cloud_organization_id="bd20fead-2c31-4392-bcd1-f1e87ad5a79c",
            ge_cloud_access_token="i_am_a_token",
        )
        assert isinstance(context, CloudDataContext)
        assert context.expectations_store_name == "new_expectations_store"
