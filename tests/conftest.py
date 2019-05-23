import pytest

import os
import json

import numpy as np

import great_expectations as ge
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
            }
        ]
    }
    return config

@pytest.fixture
def file_data_asset(tmp_path):
    path = os.path.join(tmp_path, 'file_data_asset.txt')
    with open(path, 'w+') as file:
        file.write(json.dumps([0,1,2,3,4]))

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