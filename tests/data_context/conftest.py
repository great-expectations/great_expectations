import os
import shutil

import pytest

import great_expectations as ge
from great_expectations.data_context.util import safe_mmkdir


@pytest.fixture()
def data_context_without_config_variables_filepath_configured(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp('data_context'))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    safe_mmkdir(os.path.join(asset_config_path,
                             "mydatasource/mygenerator/my_dag_node"), exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_basic_without_config_variables_filepath.yml",
                str(os.path.join(context_path, "great_expectations.yml")))
    shutil.copy("./tests/test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json",
                os.path.join(asset_config_path, "mydatasource/mygenerator/my_dag_node/default.json"))

    safe_mmkdir(os.path.join(context_path, "plugins"))
    shutil.copy("./tests/test_fixtures/custom_pandas_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")))
    shutil.copy("./tests/test_fixtures/custom_sqlalchemy_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")))
    shutil.copy("./tests/test_fixtures/custom_sparkdf_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")))
    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context_with_variables_in_config(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp('data_context'))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    safe_mmkdir(os.path.join(asset_config_path,
                             "mydatasource/mygenerator/my_dag_node"), exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_basic_with_variables.yml",
                str(os.path.join(context_path, "great_expectations.yml")))
    shutil.copy("./tests/test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json",
                os.path.join(asset_config_path, "mydatasource/mygenerator/my_dag_node/default.json"))

    safe_mmkdir(os.path.join(context_path, "plugins"))
    shutil.copy("./tests/test_fixtures/custom_pandas_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")))
    shutil.copy("./tests/test_fixtures/custom_sqlalchemy_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_sqlalchemy_dataset.py")))
    shutil.copy("./tests/test_fixtures/custom_sparkdf_dataset.py",
                str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")))
    return ge.data_context.DataContext(context_path)
