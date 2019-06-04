import pytest

import os
import shutil
import json

import sqlalchemy as sa
import pandas as pd

from great_expectations.data_context import DataContext
from great_expectations.util import safe_mmkdir
# get_data_context
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset


@pytest.fixture(scope="module")
def test_db_connection_string(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    df2 = pd.DataFrame(
        {'col_1': [0, 1, 2, 3, 4], 'col_2': ['b', 'c', 'd', 'e', 'f']})

    path = tmp_path_factory.mktemp("db_context").join("test.db")
    engine = sa.create_engine('sqlite:///' + str(path))
    df1.to_sql('table_1', con=engine, index=True)
    df2.to_sql('table_2', con=engine, index=True, schema='main')

    # Return a connection string to this newly-created db
    return 'sqlite:///' + str(path)


@pytest.fixture(scope="module")
def test_folder_connection_path(tmp_path_factory):
    df1 = pd.DataFrame(
        {'col_1': [1, 2, 3, 4, 5], 'col_2': ['a', 'b', 'c', 'd', 'e']})
    path = tmp_path_factory.mktemp("csv_context")
    df1.to_csv(path.join("test.csv"))

    return str(path)


@pytest.fixture()
def parameterized_expectations_config():
    return {
        "data_asset_name": "parameterized_expectaitons_config_fixture",
        "data_asset_type": "Dataset",
        "meta": {
        },
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_equal",
                "kwargs": {
                    "value": {
                        "$PARAMETER": "urn:great_expectations:validations:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value"
                    }
                }
            },
            {
                "expectation_type": "expect_column_unique_value_count_to_be_between",
                "kwargs": {
                    "value": {
                        "$PARAMETER": "urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value"
                    }
                }
            }
        ]
    }

@pytest.fixture()
def parameterized_config_data_context(tmp_path_factory):
    context_path = tmp_path_factory.mktemp("empty_context_dir")
    context_path = str(context_path)
    asset_config_path = os.path.join(context_path, "great_expectations/expectations")
    safe_mmkdir(asset_config_path, exist_ok=True)
    shutil.copy("./tests/test_fixtures/great_expectations_basic.yml", str(context_path))
    shutil.copy("./tests/test_fixtures/expectations/parameterized_expectations_config_fixture.json",str(asset_config_path))
    return DataContext(context_path)


# def test_invalid_data_context():
#     # Test an unknown data context name
#     with pytest.raises(ValueError) as err:
#         get_data_context('what_a_ridiculous_name', None)
#         assert "Unknown data context." in str(err)

def test_list_available_data_asset_names(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource("my_datasource", "pandas", base_directory= str(filesystem_csv))
    available_asset_names = empty_data_context.list_available_data_asset_names() 

    assert available_asset_names == [{
        "datasource": "my_datasource",
        "generators": [{
            "generator": "default",
            "available_data_asset_names": set(["f1", "f2", "f3"])
        }]
    }]
    # assert data_context.list_available_data_asset_names() == ['parameterized_expectations_config_fixture']

def test_list_expectations_configs(data_context):
    assert data_context.list_expectations_configs() == ['parameterized_expectations_config_fixture']

def test_get_existing_data_asset_config(parameterized_config_data_context):
    data_asset_config = parameterized_config_data_context.get_data_asset_config('parameterized_expectations_config_fixture')
    assert data_asset_config['data_asset_name'] == 'parameterized_expectations_config_fixture'
    assert len(data_asset_config['expectations']) == 2

def test_get_new_data_asset_config(parameterized_config_data_context):
    data_asset_config = parameterized_config_data_context.get_data_asset_config('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'this_data_asset_config_does_not_exist'
    assert len(data_asset_config['expectations']) == 0

def test_save_data_asset_config(parameterized_config_data_context):
    data_asset_config = parameterized_config_data_context.get_data_asset_config('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'this_data_asset_config_does_not_exist'
    assert len(data_asset_config['expectations']) == 0
    data_asset_config['expectations'].append({
            "expectation_type": "expect_table_row_count_to_equal",
            "kwargs": {
                "value": 10
            }
        })
    parameterized_config_data_context.save_data_asset_config(data_asset_config)
    data_asset_config_saved = parameterized_config_data_context.get_data_asset_config('this_data_asset_config_does_not_exist')
    assert data_asset_config['expectations'] == data_asset_config_saved['expectations']

# def test_sqlalchemy_data_context(test_db_connection_string):
#     context = get_data_context(
#         'SqlAlchemy', test_db_connection_string, echo=False)

#     assert context.list_datasets() == ['table_1', 'table_2']
#     dataset1 = context.get_dataset('table_1')
#     dataset2 = context.get_dataset('table_2', schema='main')
#     assert isinstance(dataset1, SqlAlchemyDataset)
#     assert isinstance(dataset2, SqlAlchemyDataset)


# def test_pandas_data_context(test_folder_connection_path):
#     context = get_data_context('PandasCSV', test_folder_connection_path)

#     assert context.list_datasets() == ['test.csv']
#     dataset = context.get_dataset('test.csv')
#     assert isinstance(dataset, PandasDataset)

def test_register_validation_results(parameterized_config_data_context):
    run_id = "460d61be-7266-11e9-8848-1681be663d3e"
    source_patient_data_results = {
        "meta": {"data_asset_name": "source_patient_data"},
        "results": [
            {
                "expectation_config": {
                    "expectation_type": "expect_table_row_count_to_equal",
                    "kwargs": {
                        "value": 1024,
                    }
                },
                "success": True,
                "exception_info": {"exception_message": None,
                    "exception_traceback": None,
                    "raised_exception": False},
                "result": {
                    "observed_value": 1024,
                    "element_count": 1024,
                    "missing_percent": 0.0,
                    "missing_count": 0
                }
            }
        ]
    }
    parameterized_config_data_context.register_validation_results(run_id, source_patient_data_results)
    bound_parameters = parameterized_config_data_context._evaluation_parameter_store.get_run_parameters(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value': 1024
    }
    source_diabetes_data_results = {
        "meta": {"data_asset_name": "source_diabetes_data"},
        "results": [
            {
                "expectation_config": {
                    "expectation_type": "expect_column_unique_value_count_to_be_between",
                    "kwargs": {
                        "column": "patient_nbr",
                        "min": 2048,
                        "max": 2048
                    }
                },
                "success": True,
                "exception_info": {"exception_message": None,
                    "exception_traceback": None,
                    "raised_exception": False},
                "result": {
                    "observed_value": 2048,
                    "element_count": 5000,
                    "missing_percent": 0.0,
                    "missing_count": 0
                }
            }
        ]
    }
    parameterized_config_data_context.register_validation_results(run_id, source_diabetes_data_results)
    bound_parameters = parameterized_config_data_context._evaluation_parameter_store.get_run_parameters(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value': 1024, 
        'urn:great_expectations:validations:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value': 2048
    }

def test_compile(parameterized_config_data_context):
    parameterized_config_data_context._compile()
    print(parameterized_config_data_context._compiled_parameters)
    assert parameterized_config_data_context._compiled_parameters == {
        'raw': {
            'urn:great_expectations:validations:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value', 
            'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value'
            }, 
        'data_assets': {
            'source_diabetes_data': {
                'expect_column_unique_value_count_to_be_between': {
                    'columns': {
                        'patient_nbr': {
                            'result': {
                                'urn:great_expectations:validations:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value'
                            }
                        }
                    }
                }
            }, 
            'source_patient_data': {
                'expect_table_row_count_to_equal': {
                    'result': {
                        'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value'
                    }
                }
            }
        }
    }

def test_normalize_data_asset_names(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("test_normalize_data_asset_names")
    base_dir = str(base_dir)
    context_dir = os.path.join(base_dir, "great_expectations")
    # asset_dir = context_dir.join("expectations/ds1/gen1/data_asset_1/")
    # os.makedirs(asset_dir)
    # with open(asset_dir("default.json"), "w") as config:
    #     json.dump({"data_asset_name": "data_assset_1"}, config)

    context = DataContext(context_dir)

    # assert context._normalize_data_asset_name("data_asset_1") == "ds1/gen1/data_asset_1"
    # NOTE: NORMALIZATION IS CURRENTLY A NO-OP
    assert context._normalize_data_asset_name("data_asset_1") == "data_asset_1"


def test_list_datasources(data_context):
    datasources = data_context.list_datasources()

    assert datasources == [
        {
            "name": "default",
            "type": "pandas"
        }
    ]

    data_context.add_datasource("second_pandas_source", "pandas")

    datasources = data_context.list_datasources()

    assert datasources == [
        {
            "name": "default",
            "type": "pandas"
        },
        {
            "name": "second_pandas_source",
            "type": "pandas"
        }
    ]