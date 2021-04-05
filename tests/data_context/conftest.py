import os
import shutil

import pytest

import great_expectations as ge
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.util import file_relative_path
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)


@pytest.fixture()
def data_context_without_config_variables_filepath_configured(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_without_config_variables_filepath.yml",
        config_variables_fixture_filename=None,
    )

    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", "ive_been_$--replaced")
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_variables.yml",
        config_variables_fixture_filename="config_variables.yml",
    )

    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config_exhaustive(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_exhaustive_variables.yml",
        config_variables_fixture_filename="config_variables_exhaustive.yml",
    )

    return ge.data_context.DataContext(context_path)


def create_data_context_files(
    context_path,
    asset_config_path,
    ge_config_fixture_filename,
    config_variables_fixture_filename=None,
):
    if config_variables_fixture_filename:
        os.makedirs(context_path, exist_ok=True)
        os.makedirs(os.path.join(context_path, "uncommitted"), exist_ok=True)
        copy_relative_path(
            f"../test_fixtures/{config_variables_fixture_filename}",
            str(os.path.join(context_path, "uncommitted/config_variables.yml")),
        )
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, "great_expectations.yml")),
        )
    else:
        os.makedirs(context_path, exist_ok=True)
        copy_relative_path(
            f"../test_fixtures/{ge_config_fixture_filename}",
            str(os.path.join(context_path, "great_expectations.yml")),
        )
    create_common_data_context_files(context_path, asset_config_path)


def create_common_data_context_files(context_path, asset_config_path):
    os.makedirs(
        os.path.join(asset_config_path, "mydatasource/mygenerator/my_dag_node"),
        exist_ok=True,
    )
    copy_relative_path(
        "../test_fixtures/"
        "expectation_suites/parameterized_expectation_suite_fixture.json",
        os.path.join(
            asset_config_path, "mydatasource/mygenerator/my_dag_node/default.json"
        ),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    copy_relative_path(
        "../test_fixtures/custom_pandas_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    copy_relative_path(
        "../test_fixtures/custom_sqlalchemy_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")),
    )
    copy_relative_path(
        "../test_fixtures/custom_sparkdf_dataset.py",
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )


def copy_relative_path(relative_src, dest):
    shutil.copy(file_relative_path(__file__, relative_src), dest)


@pytest.fixture
def basic_data_context_config():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": "plugins/",
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "does_not_have_to_be_real",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": "uncommitted/config_variables.yml",
            "datasources": {},
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
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
        }
    )
