import pytest

import shutil
import os
import json

import numpy as np
import sqlalchemy as sa

import great_expectations as ge
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context.util import safe_mmkdir

from .test_utils import get_dataset

CONTEXTS = ['PandasDataset', 'SqlAlchemyDataset', 'SparkDFDataset']


@pytest.fixture
def empty_expectations_config():
    config = {
        'dataset_name': "empty_config_fixture",
        'meta': {},
        'expectations': []
    }
    return config


@pytest.fixture
def basic_expectations_config():
    config = {
        'dataset_name': "basic_config_fixture",
        'meta': {},
        'expectations': [
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
    return config


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
            "infinities": "float",
            "nulls": "float",
            "naturals": "float"
        },
        "sqlite": {
            "infinities": "float",
            "nulls": "float",
            "naturals": "float"
        },
        "spark": {
            "infinities": "float",
            "nulls": "float",
            "naturals": "float"
        }
    }
    return get_dataset(request.param, data, schemas=schemas)


@pytest.fixture()
def sqlitedb_engine():
    return sa.create_engine('sqlite://')


@pytest.fixture()
def empty_data_context(tmp_path_factory):
    context_path = tmp_path_factory.mktemp('empty_data_context')
    context_path = str(context_path)
    context = ge.data_context.DataContext.create(context_path)
    asset_config_path = os.path.join(
        context_path, "great_expectations/expectations")
    safe_mmkdir(asset_config_path, exist_ok=True)
    return context


@pytest.fixture()
def data_context(tmp_path_factory):
    # This data_context is *manually* created to have the config we want, vs created with DataContext.create
    context_path = tmp_path_factory.mktemp('data_context')
    context_path = str(context_path)
    asset_config_path = os.path.join(
        context_path, "great_expectations/expectations")
    safe_mmkdir(asset_config_path, exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_basic.yml",
                str(os.path.join(context_path, "great_expectations/great_expectations.yml")))
    shutil.copy("./tests/test_fixtures/expectations/parameterized_expectations_config_fixture.json",
                str(asset_config_path))
    return ge.data_context.DataContext(context_path)


@pytest.fixture()
def filesystem_csv(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('test_file_kwargs_generator')
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
