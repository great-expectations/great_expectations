import os
import shutil

import great_expectations as ge
import pytest
from great_expectations.data_context.util import file_relative_path


@pytest.fixture()
def data_context_without_config_variables_filepath_configured(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(context_path, asset_config_path, False)

    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(context_path, asset_config_path, True)

    return ge.data_context.DataContext(context_path)


def create_data_context_files(
    context_path, asset_config_path, with_config_variables_file
):
    if with_config_variables_file:
        os.makedirs(context_path, exist_ok=True)
        os.makedirs(os.path.join(context_path, "uncommitted"), exist_ok=True)
        copy_relative_path(
            "../test_fixtures/config_variables.yml",
            str(os.path.join(context_path, "uncommitted/config_variables.yml")),
        )
        copy_relative_path(
            "../test_fixtures/great_expectations_basic_with_variables.yml",
            str(os.path.join(context_path, "great_expectations.yml")),
        )
    else:
        os.makedirs(context_path, exist_ok=True)
        copy_relative_path(
            "../test_fixtures/"
            "great_expectations_basic_without_config_variables_filepath.yml",
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
