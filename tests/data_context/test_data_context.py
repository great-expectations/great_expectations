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

    my_batch = not_so_empty_data_context.get_batch("f1")
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
    assert data_context.list_expectation_suites() == ['mydatasource/mygenerator/parameterized_expectation_suite_fixture/default']

def test_get_existing_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('mydatasource/mygenerator/parameterized_expectation_suite_fixture/default')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/parameterized_expectation_suite_fixture/default'
    assert len(data_asset_config['expectations']) == 2

def test_get_new_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/this_data_asset_config_does_not_exist/default'
    assert len(data_asset_config['expectations']) == 0

def test_save_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('this_data_asset_config_does_not_exist')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/this_data_asset_config_does_not_exist/default'
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

    print(data_context.get_available_data_asset_names())
    print(data_context.list_expectation_suites())


    assert True

def test_normalize_data_asset_names_delimiters(data_context):
    data_context.data_asset_name_delimiter = '.'
    assert data_context._normalize_data_asset_name("this.should.be.okay") == \
        NormalizedDataAssetName("this", "should", "be", "okay")

    data_context.data_asset_name_delimiter = '/'
    assert data_context._normalize_data_asset_name("this/should/be/okay") == \
        NormalizedDataAssetName("this", "should", "be", "okay")

    with pytest.raises(DataContextError) as exc:
        data_context.data_asset_name_delimiter = "$"
        assert "Invalid delimiter" in exc.message

    with pytest.raises(DataContextError) as exc:
        data_context.data_asset_name_delimiter = "//"
        assert "Invalid delimiter" in exc.message

def test_normalize_data_asset_names_conditions_single_name():
    pass


    # "mydatasource/mygenerator/myasset/mypurpose"
    # "notadatasource/mygenerator/myasset/mypurpose"
    # "mydatasource/myasset"
    # "myasset"
    # # Ok if only one generator has an asset with name myasset and purpose mypurpose
    # # Bad if no such generator exists or multiple generators exist
    # "mydatasource/myasset/mypurpose"

    # # Ok if only one purpose exists for myasset
    # "mydatasource/mygenerator/myasset"

    # mydatasource/
    #     default/
    #         default/
    #             default.json
    # myotherdatasource/
    #     default/
    #         default/
    #             default.json

    # "mydatasource/default/default" -> ok
    # "mydatasource/default" -> ok
    # "mydatasource/default/default/default" -> properly normaized
    # "default" -> not ok; ambiguous

    # mydatasource/
    #     default/
    #         default/
    #             default.json
    #         myotherasset/
    #             default.json
    #         mythirdasset/
    #             default.json
    #             different_purpose.json
    # myotherdatasource/
    #     default/
    #         default/
    #             default.json
    #     my_other_generator/
    #         default/
    #             default.json
    #             different_purpose.json
    # mythirddatasource/
    #     default/
    #         default/
    #             default.json
    #     my_other_generator/
    #         default/
    #             default.json
    #     my_third_generator/
    #         default/
    #             default.json
            
    # "myotherasset" -> ok. normalize to "mydatasource/default/myotherasset/default.json"
    # "mythirdasset" -> ambigous. both default and different_purpose are available
    # "myotherdatasource/default" -> ambiguous: two generators
    # "myotherdatasource/my_other_generator/default" -> ok. normalize to "myotherdatasource/my_other_generator/default/default"
    # "myotherdatasource/default/default" -> ambiguous (could be other_generator/default/default or default/default/default)
    # "myotherdatasource/default/different_purpose" -> ok. normalizse to "myotherdatasource/my_other_generator/default/different_purpose"


    # NO CONFIG, but a datasource produces: 
    #   - "mydatasource/default/myasset"
    #   - "mydatasource/default/myotherasset"
    #   - "mydatasource/myothergenerator/myasset"
    # "mydatasource/myasset/mypurpose" -> ambiguous
    # "mydatasource/default/myasset" -> ok
    # "mydatasource/default/myotherasset" -> ok

    #  - "mydatasource/myname/myname"
    # "mydatasource/myname/myname" -> ok -> "mydatasurce/myname/myname/default"



    #  - "mydatasource/myname/myname"
    #  - "mydatasource/myother/myname"
    # "mydatasource/myname/myname" -> ambigouous. could be "mydatasource/myname/myname/default" or could be "mydatasource/myother/myname/myname"

    # NO CONFIG, but a datasource produces: 
    #   - "mydatasource/mygenerator/myasset"
    #   - "mydatasource/mygenerator/myotherasset"
    #   - "mydatasource/myothergenerator/myasset"
    # "mydatasource/myasset/mypurpose" -> ambiguous


    # NO CONFIG, but a datasource produces
    #   - "mydatasource/mygenerator/myasset"
    #   - "mydatasource/mygenerator/myotherasset"
    # "mydatasource/myasset/mypurpose" -> "mydatasource/mygenerator/myasset/mypurpose"

    # tables vs queries
    # df = context.get_batch("moviedb/tables/ratings")
    # df = context.get_batch("moviedb/queries/mynewquery", query="select * from ratings limit 100")


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
    data_asset_names = titanic_data_context.profile_datasource("mydatasource")
    for data_asset_name in data_asset_names:
        validation_result = titanic_data_context.get_validation_result(data_asset_name)
        assert data_asset_name in validation_result["meta"]["data_asset_name"]
