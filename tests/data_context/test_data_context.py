import pytest

from datetime import datetime
import sys
from freezegun import freeze_time
try:
    from unittest import mock
except ImportError:
    import mock
    
import os
import shutil
import json
from glob import glob
from collections import OrderedDict

from great_expectations.exceptions import DataContextError
from great_expectations.data_context import (
    ConfigOnlyDataContext,
    DataContext,
    ExplorerDataContext,
)
from great_expectations.data_context.util import safe_mmkdir
from great_expectations.data_context.types import (
    NormalizedDataAssetName,
    ExpectationSuiteIdentifier,
)
from great_expectations.cli.init import scaffold_directories_and_notebooks
from great_expectations.dataset import PandasDataset
from great_expectations.util import gen_directory_tree_str

# from great_expectations.data_context.types import (
#     DataContextConfig,
# )
from great_expectations.data_context.store import (
    BasicInMemoryStore,
    EvaluationParameterStore,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

@pytest.fixture()
def parameterized_expectation_suite():
    with open("tests/test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json", "r") as suite:
        return json.load(suite)


def test_create_duplicate_expectation_suite(titanic_data_context):
    # create new expectation suite
    assert titanic_data_context.create_expectation_suite(data_asset_name="titanic", expectation_suite_name="test_create_expectation_suite")
    # attempt to create expectation suite with name that already exists on data asset
    with pytest.raises(DataContextError):
        titanic_data_context.create_expectation_suite(data_asset_name="titanic",
                                                      expectation_suite_name="test_create_expectation_suite")
    # create expectation suite with name that already exists on data asset, but pass overwrite_existing=True
    assert titanic_data_context.create_expectation_suite(data_asset_name="titanic", expectation_suite_name="test_create_expectation_suite", overwrite_existing=True)


def test_list_available_data_asset_names(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv))
    available_asset_names = empty_data_context.get_available_data_asset_names()

    assert available_asset_names == {
        "my_datasource": {
            "default": {"f1", "f2", "f3"}
        }
    }


def test_list_expectation_suite_keys(data_context):
    assert data_context.list_expectation_suite_keys() == [
        ExpectationSuiteIdentifier(
            data_asset_name=(
                "mydatasource",
                "mygenerator",
                "my_dag_node",
            ),
            expectation_suite_name="default"
        )
    ]

def test_get_existing_data_asset_config(data_context):
    data_asset_config = data_context.get_expectation_suite('mydatasource/mygenerator/my_dag_node', 'default')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/my_dag_node'
    assert data_asset_config['expectation_suite_name'] == 'default'
    assert len(data_asset_config['expectations']) == 2


def test_get_new_data_asset_config(data_context):
    data_asset_config = data_context.create_expectation_suite('this_data_asset_config_does_not_exist', 'default')
    assert data_asset_config['data_asset_name'] == 'mydatasource/mygenerator/this_data_asset_config_does_not_exist'
    assert data_asset_config['expectation_suite_name'] == 'default'
    assert len(data_asset_config['expectations']) == 0


def test_save_data_asset_config(data_context):
    data_asset_config = data_context.create_expectation_suite('this_data_asset_config_does_not_exist', 'default')
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


def test_evaluation_parameter_store_methods(data_context):
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

    data_context._extract_and_store_parameters_from_validation_results(
        source_patient_data_results,
        data_asset_name=source_patient_data_results["meta"]["data_asset_name"],
        expectation_suite_name=source_patient_data_results["meta"]["expectation_suite_name"],
        run_id=run_id,
    )

    bound_parameters = data_context.get_parameters_in_evaluation_parameter_store_by_run_id(run_id)
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

    data_context._extract_and_store_parameters_from_validation_results(
        source_diabetes_data_results,
        data_asset_name=source_diabetes_data_results["meta"]["data_asset_name"],
        expectation_suite_name=source_diabetes_data_results["meta"]["expectation_suite_name"],
        run_id=run_id,
    )
    bound_parameters = data_context.get_parameters_in_evaluation_parameter_store_by_run_id(run_id)
    assert bound_parameters == {
        'urn:great_expectations:validations:mydatasource/mygenerator/source_patient_data:default:expectations:expect_table_row_count_to_equal:result:observed_value': 1024,
        'urn:great_expectations:validations:mydatasource/mygenerator/source_diabetes_data:default:expectations:expect_column_unique_value_count_to_be_between:columns:patient_nbr:result:observed_value': 2048
    }

    #TODO: Add a test that specifies a data_asset_name

# FIXME : Temporarily deprecating this test, so we can develop DataSnapshotStore in a new branch.
# def test_register_validation_results_saves_data_assset_snapshot(data_context):
#     run_id = "460d61be-7266-11e9-8848-1681be663d3e"
#     source_patient_data_results = {
#         "meta": {
#             "data_asset_name": "mydatasource/mygenerator/source_patient_data",
#             "expectation_suite_name": "default"
#         },
#         "results": [
#             {
#                 "expectation_config": {
#                     "expectation_type": "expect_table_row_count_to_equal",
#                     "kwargs": {
#                         "value": 1024,
#                     }
#                 },
#                 "success": True,
#                 "exception_info": {"exception_message": None,
#                     "exception_traceback": None,
#                     "raised_exception": False},
#                 "result": {
#                     "observed_value": 1024,
#                     "element_count": 1024,
#                     "missing_percent": 0.0,
#                     "missing_count": 0
#                 }
#             }
#         ],
#         "success": False
#     }
#     data_asset = PandasDataset({"x": [1,2,3,4]})

#     snapshot_dir = os.path.join(data_context.root_directory, "uncommitted/snapshots")
#     print(snapshot_dir)

#     #The snapshot directory shouldn't exist yet
#     assert not os.path.isfile(snapshot_dir)

#     data_context.add_store(
#         "data_asset_snapshot_store",
#         {
#             "module_name": "great_expectations.data_context.store",
#             "class_name": "NamespacedReadWriteStore",
#             "serialization_type" : "pandas_csv",
#             "resource_identifier_class_name": "ValidationResultIdentifier",
#             "store_backend" : {
#                 "module_name": "great_expectations.data_context.store",
#                 "class_name": "FixedLengthTupleFilesystemStoreBackend",
#                 "base_directory" : "uncommitted/snapshots",
#                 "filepath_template": "{4}/{0}/{1}/{2}/validation-results-{2}-{3}-{4}.{file_extension}",
#                 "file_extension" : "csv.gz",
#                 # "compression" : "gzip",
#             }
#         }
#     )
#     # print(json.dumps(data_context._project_config, indent=2))
    
#     #The snapshot directory shouldn't contain any files
#     # assert len(glob(snapshot_dir+"/*/*/*/*/*.csv.gz")) == 0
#     print(gen_directory_tree_str(snapshot_dir))
#     assert gen_directory_tree_str(snapshot_dir) == ""

#     res = data_context.register_validation_results(
#         run_id,
#         source_patient_data_results,
#         data_asset=data_asset
#     )
    
#     #This snapshot directory should now exist
#     assert os.path.isdir(snapshot_dir)

#     #we should have one file created as a side effect
#     print(gen_directory_tree_str(snapshot_dir))
#     glob_results = glob(snapshot_dir+"/*/*/*/*/*.csv.gz")
#     print(glob_results)
#     assert len(glob_results) == 1


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
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv))
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
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv))
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
    data_context.add_datasource("my_second_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=second_datasource_basedir)

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
    suite = data_context.create_expectation_suite("my_datasource/in_memory_generator/f1", "default")
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

    assert OrderedDict(datasources) == OrderedDict([

        {
            'name': 'mydatasource',
            'class_name': 'PandasDatasource'
        }
    ])

    data_context.add_datasource("second_pandas_source",
                           module_name="great_expectations.datasource",
                           class_name="PandasDatasource",
                           )

    datasources = data_context.list_datasources()

    assert OrderedDict(datasources) == OrderedDict([
        {
            'name': 'mydatasource',
            'class_name': 'PandasDatasource'
        },
        {
            'name': 'second_pandas_source',
            'class_name': 'PandasDatasource'
        }
    ])


def test_data_context_result_store(titanic_data_context):
    """
    Test that validation results can be correctly fetched from the configured results store
    """
    print(titanic_data_context.stores["local_validation_result_store"].list_keys())

    profiling_results = titanic_data_context.profile_datasource("mydatasource")

    print(titanic_data_context.stores["local_validation_result_store"].list_keys())

    for profiling_result in profiling_results['results']:
        data_asset_name = profiling_result[1]['meta']['data_asset_name']
        validation_result = titanic_data_context.get_validation_result(data_asset_name, "BasicDatasetProfiler")
        assert data_asset_name in validation_result["meta"]["data_asset_name"]

    all_validation_result = titanic_data_context.get_validation_result(
        "mydatasource/mygenerator/Titanic",
        "BasicDatasetProfiler",
    )
    assert len(all_validation_result["results"]) == 51

    failed_validation_result = titanic_data_context.get_validation_result(
        "mydatasource/mygenerator/Titanic",
        "BasicDatasetProfiler",
        failed_only=True,
    )
    assert len(failed_validation_result["results"]) == 8


def test_render_full_static_site_from_empty_project(tmp_path_factory, filesystem_csv_3):

    # TODO : Use a standard test fixture
    # TODO : Have that test fixture copy a directory, rather than building a new one from scratch

    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)

    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data/titanic"))
    shutil.copy(
        "./tests/test_sets/Titanic.csv",
        str(os.path.join(project_dir, "data/titanic/Titanic.csv"))
    )

    os.makedirs(os.path.join(project_dir, "data/random"))
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data/random/f1.csv"))
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data/random/f2.csv"))
    )

    assert gen_directory_tree_str(project_dir) == """\
project_path/
    data/
        random/
            f1.csv
            f2.csv
        titanic/
            Titanic.csv
"""

    context = DataContext.create(project_dir)
    ge_directory = os.path.join(project_dir, "great_expectations")
    scaffold_directories_and_notebooks(ge_directory)
    context.add_datasource("titanic",
                            module_name="great_expectations.datasource",
                            class_name="PandasDatasource",
                            base_directory=os.path.join(project_dir, "data/titanic/"))

    context.add_datasource("random",
                            module_name="great_expectations.datasource",
                            class_name="PandasDatasource",
                            base_directory=os.path.join(project_dir, "data/random/"))

    context.profile_datasource("titanic")
    assert gen_directory_tree_str(project_dir) == """\
project_path/
    data/
        random/
            f1.csv
            f2.csv
        titanic/
            Titanic.csv
    great_expectations/
        .gitignore
        great_expectations.yml
        datasources/
        expectations/
            titanic/
                default/
                    Titanic/
                        BasicDatasetProfiler.json
        fixtures/
        notebooks/
            create_expectations.ipynb
            integrate_validation_into_pipeline.ipynb
        plugins/
        uncommitted/
            config_variables.yml
            documentation/
                local_site/
            samples/
            validations/
                profiling/
                    titanic/
                        default/
                            Titanic/
                                BasicDatasetProfiler.json
"""

    context.profile_datasource("random")
    context.build_data_documentation()

    observed = gen_directory_tree_str(os.path.join(project_dir, "great_expectations/uncommitted/documentation"))
    assert observed == """\
documentation/
    local_site/
        index.html
        expectations/
            random/
                default/
                    f1/
                        BasicDatasetProfiler.html
                    f2/
                        BasicDatasetProfiler.html
            titanic/
                default/
                    Titanic/
                        BasicDatasetProfiler.html
        validations/
            profiling/
                random/
                    default/
                        f1/
                            BasicDatasetProfiler.html
                        f2/
                            BasicDatasetProfiler.html
                titanic/
                    default/
                        Titanic/
                            BasicDatasetProfiler.html
"""

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


def test_move_validation_to_fixtures(titanic_data_context):
    profiling_results = titanic_data_context.profile_datasource("mydatasource")
    all_validation_result = titanic_data_context.get_validation_result(
        "mydatasource/mygenerator/Titanic",
        "BasicDatasetProfiler",
        "profiling"
    )
    # print(all_validation_result)
    assert len(all_validation_result["results"]) == 51

    assert titanic_data_context.stores["fixture_validation_results_store"].list_keys() == []

    titanic_data_context.move_validation_to_fixtures(
        "mydatasource/mygenerator/Titanic",
        "BasicDatasetProfiler",
        "profiling"
    )

    # titanic_data_context.stores["fixtures"].get(
    #     "mydatasource/mygenerator/Titanic",
    #     "BasicDatasetProfiler",
    #     "profiling"
    # )
    assert len(titanic_data_context.stores["fixture_validation_results_store"].list_keys()) == 1


def test_add_store(empty_data_context):
    assert "my_new_store" not in empty_data_context.stores.keys()
    assert "my_new_store" not in empty_data_context.get_config()["stores"]
    new_store = empty_data_context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "BasicInMemoryStore",
        }
    )
    assert "my_new_store" in empty_data_context.stores.keys()
    assert "my_new_store" in empty_data_context.get_config()["stores"]

    assert isinstance(new_store, BasicInMemoryStore)


@pytest.fixture
def basic_data_context_config():
    # return DataContextConfig(**{
    return {
        "config_version": 1,
        "plugins_directory": "plugins/",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "profiling_store_name": "does_not_have_to_be_real",
        "expectations_store_name": "expectations_store",
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationStore",
                "store_backend": {
                    "class_name": "FixedLengthTupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "EvaluationParameterStore",
            }
        },
        "data_docs_sites": {},
        "validation_operators": {
            "default": {
                "class_name": "PerformActionListValidationOperator",
                "action_list": []
            }
        }
    }


def test_ExplorerDataContext(titanic_data_context):
    context_root_directory = titanic_data_context.root_directory
    explorer_data_context = ExplorerDataContext(context_root_directory)
    assert explorer_data_context._expectation_explorer_manager
    

@freeze_time("2012-01-14")
def test_ExplorerDataContext_expectation_widget(titanic_data_context):
    context_root_directory = titanic_data_context.root_directory
    explorer_data_context = ExplorerDataContext(context_root_directory)
    explorer_data_context.create_expectation_suite('Titanic', expectation_suite_name='my_suite')
    data_asset = explorer_data_context.get_batch('Titanic', expectation_suite_name='my_suite',
                                                 batch_kwargs=explorer_data_context.yield_batch_kwargs("Titanic"))
    widget_output = data_asset.expect_column_to_exist('test')
    print(widget_output)
    if sys.version[0:3] == '2.7':
        expected_widget_output = "Accordion(children=(VBox(children=(HBox(children=(VBox(children=(HTML(value=u'<div><strong>Data Asset Name: </strong>mydatasource/mygenerator/Titanic</div>'), HTML(value=u'<div><strong>Column: </strong>test</div>'), HTML(value=u'<span><strong>Expectation Type: </strong>expect_column_to_exist</span>'), HTML(value=u'<span><strong>Success: </strong>False</span>'), HTML(value=u'<div><strong>Date/Time Validated (UTC): </strong>2012-01-14 00:00</div>')), layout=Layout(margin=u'10px', width=u'40%')), VBox(children=(Text(value=u'', description=u'<strong>column_index: </strong>', description_tooltip=u'', layout=Layout(width=u'400px'), placeholder=u'press enter to confirm...', style=DescriptionStyle(description_width=u'150px')),), layout=Layout(margin=u'10px', width=u'60%')))), Accordion(children=(Output(),), _titles={u'0': 'Exceptions/Warnings'}), Accordion(children=(VBox(),), selected_index=None, _titles={u'0': 'Validation Result Details'}), Button(button_style=u'danger', description=u'Remove Expectation', icon=u'trash', layout=Layout(width=u'auto'), style=ButtonStyle(), tooltip=u'click to remove expectation'))),), layout=Layout(border=u'2px solid red', margin=u'5px'), _titles={u'0': 'test | expect_column_to_exist'})"
    else:
        expected_widget_output = "Accordion(children=(VBox(children=(HBox(children=(VBox(children=(HTML(value='<div><strong>Data Asset Name: </strong>mydatasource/mygenerator/Titanic</div>'), HTML(value='<div><strong>Column: </strong>test</div>'), HTML(value='<span><strong>Expectation Type: </strong>expect_column_to_exist</span>'), HTML(value='<span><strong>Success: </strong>False</span>'), HTML(value='<div><strong>Date/Time Validated (UTC): </strong>2012-01-14 00:00</div>')), layout=Layout(margin='10px', width='40%')), VBox(children=(Text(value='', description='<strong>column_index: </strong>', description_tooltip='', layout=Layout(width='400px'), placeholder='press enter to confirm...', style=DescriptionStyle(description_width='150px')),), layout=Layout(margin='10px', width='60%')))), Accordion(children=(Output(),), _titles={'0': 'Exceptions/Warnings'}), Accordion(children=(VBox(),), selected_index=None, _titles={'0': 'Validation Result Details'}), Button(button_style='danger', description='Remove Expectation', icon='trash', layout=Layout(width='auto'), style=ButtonStyle(), tooltip='click to remove expectation'))),), layout=Layout(border='2px solid red', margin='5px'), _titles={'0': 'test | expect_column_to_exist'})"
    assert str(widget_output) == expected_widget_output


def test_ConfigOnlyDataContext__initialization(tmp_path_factory, basic_data_context_config):
    config_path = str(tmp_path_factory.mktemp('test_ConfigOnlyDataContext__initialization__dir'))
    context = ConfigOnlyDataContext(
        basic_data_context_config,
        config_path,
    )

    assert context.root_directory.split("/")[-1] == "test_ConfigOnlyDataContext__initialization__dir0"
    assert context.plugins_directory.split("/")[-3:] == ["test_ConfigOnlyDataContext__initialization__dir0", "plugins",""]


def test_evaluation_parameter_store_methods(basic_data_context_config):
    context = ConfigOnlyDataContext(
        basic_data_context_config,
        "testing",
    )

    assert isinstance(context.evaluation_parameter_store, EvaluationParameterStore)

    assert context.get_parameters_in_evaluation_parameter_store_by_run_id("foo") == {}
    context.set_parameters_in_evaluation_parameter_store_by_run_id_and_key("foo", "bar", "baz")
    assert context.get_parameters_in_evaluation_parameter_store_by_run_id("foo") == {
        "bar" : "baz"
    }

    context.set_parameters_in_evaluation_parameter_store_by_run_id_and_key("foo", "car", "caz")
    assert context.get_parameters_in_evaluation_parameter_store_by_run_id("foo") == {
        "bar" : "baz",
        "car" : "caz"
    }

    context.set_parameters_in_evaluation_parameter_store_by_run_id_and_key("goo", "dar", "daz")
    assert context.get_parameters_in_evaluation_parameter_store_by_run_id("foo") == {
        "bar" : "baz",
        "car" : "caz"
    }
    assert context.get_parameters_in_evaluation_parameter_store_by_run_id("goo") == {
        "dar" : "daz",
    }

def test__normalize_absolute_or_relative_path(tmp_path_factory, basic_data_context_config):
    config_path = str(tmp_path_factory.mktemp('test__normalize_absolute_or_relative_path__dir'))
    context = ConfigOnlyDataContext(
        basic_data_context_config,
        config_path,
    )

    print(context._normalize_absolute_or_relative_path("yikes"))
    assert "test__normalize_absolute_or_relative_path__dir0/yikes" in context._normalize_absolute_or_relative_path("yikes") 

    context._normalize_absolute_or_relative_path("/yikes")
    assert "test__normalize_absolute_or_relative_path__dir" not in context._normalize_absolute_or_relative_path("/yikes") 
    assert "/yikes" == context._normalize_absolute_or_relative_path("/yikes") 


def test__get_normalized_data_asset_name_filepath(basic_data_context_config):
    context = ConfigOnlyDataContext(
        project_config=basic_data_context_config,
        context_root_dir="testing/",
    )
    assert context._get_normalized_data_asset_name_filepath(
        NormalizedDataAssetName("my_db", "default", "my_table"),
        "default",
        "my/base/path",
        ".json"
    ) == "my/base/path/my_db/default/my_table/default.json"