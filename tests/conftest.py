import pytest

import shutil
import os
import json
import warnings

import numpy as np
import sqlalchemy as sa

import great_expectations as ge
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context.util import safe_mmkdir

from .test_utils import get_dataset

CONTEXTS = ['PandasDataset', 'sqlite', 'SparkDFDataset']

### TODO: make it easier to turn off Spark as well

#####
#
# Postgresql Context
#
#####
try:
    engine = sa.create_engine('postgresql://postgres@localhost/test_ci')
    conn = engine.connect()
    CONTEXTS += ['postgresql']
except (ImportError, sa.exc.SQLAlchemyError):
    warnings.warn("No postgres context available for testing.")

#####
#
# MySQL context -- TODO FIXME enable these tests
#
#####

# try:
#     engine = sa.create_engine('mysql://root@localhost/test_ci')
#     conn = engine.connect()
#     CONTEXTS += ['mysql']
# except (ImportError, sa.exc.SQLAlchemyError):
#     warnings.warn("No mysql context available for testing.")

@pytest.fixture
def empty_expectation_suite():
    expectation_suite = {
        'dataset_name': "empty_suite_fixture",
        'meta': {},
        'expectations': []
    }
    return expectation_suite


@pytest.fixture
def basic_expectation_suite():
    expectation_suite = {
        'dataset_name': "basic_suite_fixture",
        'meta': {},
        'expectations': [
            # Removing this from list of expectations, since mysql doesn't support infinities and we want generic fixtures
            # TODO: mysql cannot handle columns with infinities....re-handle this case
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "infinities"
                }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "nulls"
                }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "naturals"
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {
                    "column": "naturals"
                }
            }
        ]
    }
    return expectation_suite


@pytest.fixture
def file_data_asset(tmp_path):
    tmp_path = str(tmp_path)
    path = os.path.join(tmp_path, 'file_data_asset.txt')
    with open(path, 'w+') as file:
        file.write(json.dumps([0, 1, 2, 3, 4]))

    return ge.data_asset.FileDataAsset(file_path=path)


@pytest.fixture(params=CONTEXTS)
def dataset(request):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""
    # No infinities for mysql
    if request.param == "mysql":
        data = {
            # "infinities": [-np.inf, -10, -np.pi, 0, np.pi, 10/2.2, np.inf],
            "nulls": [np.nan, None, 0, 1.1, 2.2, 3.3, None],
            "naturals": [1, 2, 3, 4, 5, 6, 7]
        }
    else:
        data = {
            "infinities": [-np.inf, -10, -np.pi, 0, np.pi, 10/2.2, np.inf],
            "nulls": [np.nan, None, 0, 1.1, 2.2, 3.3, None],
            "naturals": [1, 2, 3, 4, 5, 6, 7]
        }
    schemas = {
        "pandas": {
            "infinities": "float64",
            "nulls": "float64",
            "naturals": "float64"
        },
        "postgresql": {
            "infinities": "DOUBLE_PRECISION",
            "nulls": "DOUBLE_PRECISION",
            "naturals": "DOUBLE_PRECISION"
        },
        "sqlite": {
            "infinities": "FLOAT",
            "nulls": "FLOAT",
            "naturals": "FLOAT"
        },
        "mysql": {
            "infinities": "FLOAT",
            "nulls": "FLOAT",
            "naturals": "FLOAT"
        },
        "spark": {
            "infinities": "FloatType",
            "nulls": "FloatType",
            "naturals": "FloatType"
        }
    }
    return get_dataset(request.param, data, schemas=schemas)


@pytest.fixture()
def sqlitedb_engine():
    return sa.create_engine('sqlite://')


@pytest.fixture()
def empty_data_context(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('empty_data_context'))
    context = ge.data_context.DataContext.create(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(
        context_path, "expectations")
    safe_mmkdir(asset_config_path, exist_ok=True)
    return context


@pytest.fixture
def titanic_data_context(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('titanic_data_context'))
    context_path = os.path.join(project_path, "great_expectations")
    safe_mmkdir(os.path.join(context_path, "expectations"), exist_ok=True)
    safe_mmkdir(os.path.join(context_path, "unexpected/validations"), exist_ok=True)
    data_path = os.path.join(context_path, "../data")
    safe_mmkdir(os.path.join(data_path), exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_titanic.yml", str(os.path.join(context_path, "great_expectations.yml")))
    shutil.copy("./tests/test_sets/Titanic.csv", str(os.path.join(context_path, "../data/Titanic.csv")))
    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def data_context(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    project_path = str(tmp_path_factory.mktemp('data_context'))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    safe_mmkdir(os.path.join(asset_config_path, "mydatasource/mygenerator/parameterized_expectation_suite_fixture"), exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_basic.yml", str(os.path.join(context_path, "great_expectations.yml")))
    shutil.copy("./tests/test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json", 
        os.path.join(asset_config_path, "mydatasource/mygenerator/parameterized_expectation_suite_fixture/default.json"))
    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def filesystem_csv(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('filesystem_csv')
    base_dir = str(base_dir)
    # Put a few files in the directory
    with open(os.path.join(base_dir, "f1.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])
    with open(os.path.join(base_dir, "f2.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])

    safe_mmkdir(os.path.join(base_dir, "f3"))
    with open(os.path.join(base_dir, "f3", "f3_20190101.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])
    with open(os.path.join(base_dir, "f3", "f3_20190102.csv"), "w") as outfile:
        outfile.writelines(["a,b,c\n"])

    return base_dir


@pytest.fixture()
def filesystem_csv_2(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('test_files')
    base_dir = str(base_dir)

    # Put a file in the directory
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    toy_dataset.to_csv(os.path.join(base_dir, "f1.csv"), index=None)
    return base_dir
