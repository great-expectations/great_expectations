import pytest

from datetime import datetime
try:
    from unittest import mock
except ImportError:
    import mock
    
import os
import shutil
import json

import sqlalchemy as sa
import pandas as pd

from great_expectations.exceptions import DataContextError
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import safe_mmkdir, NormalizedDataAssetName
from great_expectations.dataset import PandasDataset, SqlAlchemyDataset


@pytest.fixture()
def parameterized_expectation_suite():
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

def test_validate_saves_result_inserts_run_id(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv))
    not_so_empty_data_context = empty_data_context

    # we should now be able to validate, and have validations saved.
    assert not_so_empty_data_context._project_config["result_store"]["filesystem"]["base_directory"] == "uncommitted/validations/"

    my_batch = not_so_empty_data_context.get_batch("my_datasource/f1")

    my_batch.expect_column_to_exist("a")

    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        validation_result = my_batch.validate()

    with open(os.path.join(not_so_empty_data_context.root_directory, 
              "uncommitted/validations/1955-11-05T00:00:00/my_datasource/default/f1/default.json")) as infile:
        saved_validation_result = json.load(infile)
    
    assert validation_result == saved_validation_result

def test_list_available_data_asset_names(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource("my_datasource", "pandas", base_directory= str(filesystem_csv))
    available_asset_names = empty_data_context.get_available_data_asset_names()

    assert available_asset_names == {
        "my_datasource": {
            "default": set(["f1", "f2", "f3"])
        }
    }

def test_list_expectation_suites(data_context):
    assert data_context.list_expectation_suites() == {
        "mydatasource" : {
            "mygenerator": {
                "parameterized_expectation_suite_fixture": ["default"]
            }
        }
    }

def test_get_existing_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('mydatasource/mygenerator/parameterized_expectation_suite_fixture', 'default')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/parameterized_expectation_suite_fixture'
    assert data_asset_config['expectation_suite_name'] == 'default'
    assert len(data_asset_config['expectations']) == 2

def test_get_new_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/this_data_asset_config_does_not_exist'
    assert data_asset_config['expectation_suite_name'] == 'default'
    assert len(data_asset_config['expectations']) == 0

def test_save_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/this_data_asset_config_does_not_exist'
    assert data_asset_config["expectation_suite_name"] == "default"
    assert len(data_asset_config['expectations']) == 0
    data_asset_config['expectations'].append({
            "expectation_type": "expect_table_row_count_to_equal",
            "kwargs": {
                "value": 10
            }
        })
    data_context.save_expectation_suite(data_asset_config)
    data_asset_config_saved = data_context.get_expectation_suite('this_data_asset_config_does_not_exist')
    assert data_asset_config['expectations'] == data_asset_config_saved['expectations']

def test_register_validation_results(data_context):
    run_id = "460d61be-7266-11e9-8848-1681be663d3e"
    source_patient_data_results = {
        "meta": {
            "data_asset_name": "source_patient_data",
            "expectation_suite_name": "default"
        },
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
        ],
        "success": True
    }
    res = data_context.register_validation_results(run_id, source_patient_data_results)
    assert res == source_patient_data_results # results should always be returned, and in this case not modified
    bound_parameters = data_context._evaluation_parameter_store.get_run_parameters(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value': 1024
    }
    source_diabetes_data_results = {
        "meta": {
            "data_asset_name": "source_diabetes_data",
            "expectation_suite_name": "default"
        },
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
        ],
        "success": True
    }
    data_context.register_validation_results(run_id, source_diabetes_data_results)
    bound_parameters = data_context._evaluation_parameter_store.get_run_parameters(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:source_patient_data:expectations:expect_table_row_count_to_equal:result:observed_value': 1024, 
        'urn:great_expectations:validations:source_diabetes_data:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value': 2048
    }

def test_compile(data_context):
    data_context._compile()
    assert data_context._compiled_parameters == {
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

def test_normalize_data_asset_names_error(data_context):
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("this/should/never/work/because/it/is/so/long")
        assert "found too many components using delimiter '/'" in exc.message

def test_normalize_data_asset_names_delimiters(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv))
    data_context = empty_data_context

    data_context.data_asset_name_delimiter = '.'
    assert data_context._normalize_data_asset_name("my_datasource.default.f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    data_context.data_asset_name_delimiter = '/'
    assert data_context._normalize_data_asset_name("my_datasource/default/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    with pytest.raises(DataContextError) as exc:
        data_context.data_asset_name_delimiter = "$"
        assert "Invalid delimiter" in exc.message

    with pytest.raises(DataContextError) as exc:
        data_context.data_asset_name_delimiter = "//"
        assert "Invalid delimiter" in exc.message

def test_normalize_data_asset_names_conditions(empty_data_context, filesystem_csv, tmp_path_factory):
    # If no datasource is configured, nothing should be allowed to normalize:
    with pytest.raises(DataContextError) as exc:    
        empty_data_context._normalize_data_asset_name("f1")
        assert "No datasource configured" in exc.message

    with pytest.raises(DataContextError) as exc:    
        empty_data_context._normalize_data_asset_name("my_datasource/f1")
        assert "No datasource configured" in exc.message

    with pytest.raises(DataContextError) as exc:    
        empty_data_context._normalize_data_asset_name("my_datasource/default/f1")
        assert "No datasource configured" in exc.message

    ###
    # Add a datasource
    ###
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv))
    data_context = empty_data_context

    # We can now reference existing or available data asset namespaces using
    # a the data_asset_name; the datasource name and data_asset_name or all
    # three components of the normalized data asset name
    assert data_context._normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context._normalize_data_asset_name("my_datasource/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context._normalize_data_asset_name("my_datasource/default/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    # With only one datasource and generator configured, we
    # can create new namespaces at the generator asset level easily:
    assert data_context._normalize_data_asset_name("f5") == \
        NormalizedDataAssetName("my_datasource", "default", "f5")

    # We can also be more explicit in creating new namespaces at the generator asset level:
    assert data_context._normalize_data_asset_name("my_datasource/f6") == \
        NormalizedDataAssetName("my_datasource", "default", "f6")

    assert data_context._normalize_data_asset_name("my_datasource/default/f7") == \
        NormalizedDataAssetName("my_datasource", "default", "f7")

    # However, we cannot create against nonexisting datasources or generators:
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("my_fake_datasource/default/f7")
        assert "no configured datasource 'my_fake_datasource' with generator 'default'" in exc.message
    
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("my_datasource/my_fake_generator/f7")
        assert "no configured datasource 'my_datasource' with generator 'my_fake_generator'" in exc.message
    
    ###
    # Add a second datasource
    ###

    second_datasource_basedir = str(tmp_path_factory.mktemp("test_normalize_data_asset_names_conditions_single_name"))
    with open(os.path.join(second_datasource_basedir, "f3.tsv"), "w") as outfile:
        outfile.write("\n\n\n")
    with open(os.path.join(second_datasource_basedir, "f4.tsv"), "w") as outfile:
        outfile.write("\n\n\n")
    data_context.add_datasource(
        "my_second_datasource", "pandas", base_directory=second_datasource_basedir)

    # We can still reference *unambiguous* data_asset_names:
    assert data_context._normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")
    
    assert data_context._normalize_data_asset_name("f4") == \
        NormalizedDataAssetName("my_second_datasource", "default", "f4")

    # However, single-name resolution will fail with ambiguous entries
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("f3")
        assert "Ambiguous data_asset_name 'f3'. Multiple candidates found" in exc.message

    # Two-name resolution still works since generators are not ambiguous in that case
    assert data_context._normalize_data_asset_name("my_datasource/f3") == \
        NormalizedDataAssetName("my_datasource", "default", "f3")
    
    # We can also create new namespaces using only two components since that is not ambiguous
    assert data_context._normalize_data_asset_name("my_datasource/f9") == \
        NormalizedDataAssetName("my_datasource", "default", "f9")

    # However, we cannot create new names using only a single component
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("f10")
        assert "Ambiguous data_asset_name: no existing data_asset has the provided name" in exc.message

    ###
    # Add a second generator to one datasource
    ###
    my_datasource = data_context.get_datasource("my_datasource")
    my_datasource.add_generator("in_memory_generator", "memory")

    # We've chosen an interesting case: in_memory_generator does not by default provide its own names
    # so we can still get some names if there is no ambiguity about the namespace
    assert data_context._normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    # However, if we add a data_asset that would cause that name to be ambiguous, it will then fail:
    suite = data_context.get_expectation_suite("my_datasource/in_memory_generator/f1")
    data_context.save_expectation_suite(suite)

    with pytest.raises(DataContextError) as exc:
        name = data_context._normalize_data_asset_name("f1")
        assert "Ambiguous data_asset_name 'f1'. Multiple candidates found" in exc.message

    # It will also fail with two components since there is still ambiguity:
    with pytest.raises(DataContextError) as exc:
        data_context._normalize_data_asset_name("my_datasource/f1")
        assert "Ambiguous data_asset_name 'f1'. Multiple candidates found" in exc.message

    # But we can get the asset using all three components
    assert data_context._normalize_data_asset_name("my_datasource/default/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context._normalize_data_asset_name("my_datasource/in_memory_generator/f1") == \
        NormalizedDataAssetName("my_datasource", "in_memory_generator", "f1")


def test_list_datasources(data_context):
    datasources = data_context.list_datasources()

    assert datasources == [
        {
            "name": "mydatasource",
            "type": "pandas"
        }
    ]

    data_context.add_datasource("second_pandas_source", "pandas")

    datasources = data_context.list_datasources()

    assert datasources == [
        {
            "name": "mydatasource",
            "type": "pandas"
        },
        {
            "name": "second_pandas_source",
            "type": "pandas"
        }
    ]

def test_data_context_result_store(titanic_data_context):
    """
    Test that validation results can be correctly fetched from the configured results store
    """
    profiling_results = titanic_data_context.profile_datasource("mydatasource")
    for profiling_result in profiling_results:
        data_asset_name = profiling_result[1]['meta']['data_asset_name']
        validation_result = titanic_data_context.get_validation_result(data_asset_name, "BasicDatasetProfiler")
        assert data_asset_name in validation_result["meta"]["data_asset_name"]
