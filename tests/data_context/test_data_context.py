import pytest

from datetime import datetime
try:
    from unittest import mock
except ImportError:
    import mock
    
import os
import shutil
import json
from glob import glob

from great_expectations.exceptions import DataContextError
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import (NormalizedDataAssetName, safe_mmkdir)
from great_expectations.cli.init import scaffold_directories_and_notebooks


@pytest.fixture()
def parameterized_expectation_suite():
    with open("tests/test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json", "r") as suite:
        return json.load(suite)


def test_validate_saves_result_inserts_run_id(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv))
    not_so_empty_data_context = empty_data_context

    # we should now be able to validate, and have validations saved.
    assert not_so_empty_data_context._project_config["validations_store"]["local"]["base_directory"] == \
        "uncommitted/validations/"

    my_batch = not_so_empty_data_context.get_batch("my_datasource/f1")

    my_batch.expect_column_to_exist("a")

    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        validation_result = my_batch.validate()

    with open(os.path.join(not_so_empty_data_context.root_directory, 
              "uncommitted/validations/1955-11-05T000000Z/my_datasource/default/f1/default.json")) as infile:
        saved_validation_result = json.load(infile)
    
    assert validation_result == saved_validation_result


def test_list_available_data_asset_names(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource("my_datasource", "pandas", base_directory= str(filesystem_csv))
    available_asset_names = empty_data_context.get_available_data_asset_names()

    assert available_asset_names == {
        "my_datasource": {
            "default": {"f1", "f2", "f3"}
        }
    }


def test_list_expectation_suites(data_context):
    assert data_context.list_expectation_suites() == {
        "mydatasource": {
            "mygenerator": {
                "my_dag_node": ["default"]
            }
        }
    }


def test_get_existing_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('mydatasource/mygenerator/my_dag_node', 'default')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/my_dag_node'
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
            "data_asset_name": "mydatasource/mygenerator/source_patient_data",
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
    assert res == source_patient_data_results  # results should always be returned, and in this case not modified
    bound_parameters = data_context._evaluation_parameter_store.get_run_parameters(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:result:observed_value': 1024
    }
    source_diabetes_data_results = {
        "meta": {
            "data_asset_name": "mydatasource/mygenerator/source_diabetes_data",
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
        'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:result:observed_value': 1024,
        'urn:great_expectations:validations:mydatasource/mygenerator/source_diabetes_data:default:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value': 2048
    }


def test_compile(data_context):
    data_context._compile()
    assert data_context._compiled_parameters == {
        'raw': {
            'urn:great_expectations:validations:mydatasource/mygenerator/source_diabetes_data:default:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value',
            'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:result:observed_value'
            }, 
        'data_assets': {
            'mydatasource/mygenerator/source_diabetes_data': {
                'default': {
                    'expect_column_unique_value_count_to_be_between': {
                        'columns': {
                            'patient_nbr': {
                                'result': {
                                    'urn:great_expectations:validations:mydatasource/mygenerator/source_diabetes_data:default:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value'
                                }
                            }
                        }
                    }
                }
            }, 
            'mydatasource/mygenerator/source_patient_data': {
                'default': {
                    'expect_table_row_count_to_equal': {
                        'result': {
                            'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:result:observed_value'
                        }
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
    for profiling_result in profiling_results['results']:
        data_asset_name = profiling_result[1]['meta']['data_asset_name']
        validation_result = titanic_data_context.get_validation_result(data_asset_name, "BasicDatasetProfiler")
        assert data_asset_name in validation_result["meta"]["data_asset_name"]


def test_render_full_static_site(tmp_path_factory, filesystem_csv_3):
    project_dir = str(tmp_path_factory.mktemp("project_dir"))
    print(project_dir)

    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data/titanic"))
    curdir = os.path.abspath(os.getcwd())
    shutil.copy(
        "./tests/test_sets/Titanic.csv",
        str(os.path.join(project_dir, "data/titanic/Titanic.csv"))
    )

    os.makedirs(os.path.join(project_dir, "data/random"))
    curdir = os.path.abspath(os.getcwd())
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data/random/f1.csv"))
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data/random/f2.csv"))
    )

    context = DataContext.create(project_dir)
    ge_directory = os.path.join(project_dir, "great_expectations")
    scaffold_directories_and_notebooks(ge_directory)
    context.add_datasource(
        "titanic",
        "pandas",
        base_directory=os.path.join(project_dir, "data/titanic/")
    )
    context.add_datasource(
        "random",
        "pandas",
        base_directory=os.path.join(project_dir, "data/random/")
    )

    context.profile_datasource("titanic")

    context.profile_datasource("random")

    context.build_data_documentation()

    # Titanic

    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/validations/profiling/titanic/default/Titanic/BasicDatasetProfiler.json"
    ))

    assert os.path.exists(os.path.join( # profiling results HTML
        ge_directory,
        "uncommitted/documentation/local_site/profiling/titanic/default/Titanic/BasicDatasetProfiler.html"
    ))
    
    assert os.path.exists(os.path.join( # profiling expectations HTML
        ge_directory,
        "uncommitted/documentation/local_site/expectations/titanic/default/Titanic/BasicDatasetProfiler.html"
    ))

    # f1

    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/validations/profiling/random/default/f1/BasicDatasetProfiler.json"
    ))
    assert os.path.exists(os.path.join( # profiling results HTML
        ge_directory,
        "uncommitted/documentation/local_site/profiling/random/default/f1/BasicDatasetProfiler.html"
    ))
    
    assert os.path.exists(os.path.join( # profiling expectations HTML
        ge_directory,
        "uncommitted/documentation/local_site/profiling/random/default/f1/BasicDatasetProfiler.html"
    ))

    # f2

    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/validations/profiling/random/default/f2/BasicDatasetProfiler.json"
    ))
    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/documentation/local_site/profiling/random/default/f2/BasicDatasetProfiler.html"
    ))

    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/documentation/local_site/expectations/random/default/f2/BasicDatasetProfiler.html"
    ))

    # local_site index.html
    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/documentation/local_site/index.html"
    ))

    # team_site index.html
    assert os.path.exists(os.path.join(
        ge_directory,
        "uncommitted/documentation/team_site/index.html"
    ))

    # save documentation locally
    safe_mmkdir("./tests/data_context/output")
    safe_mmkdir("./tests/data_context/output/documentation")
    
    if os.path.isdir("./tests/data_context/output/documentation"):
        shutil.rmtree("./tests/data_context/output/documentation")
    shutil.copytree(
        os.path.join(
            ge_directory,
            "uncommitted/documentation/"
        ),
        "./tests/data_context/output/documentation"
    )
