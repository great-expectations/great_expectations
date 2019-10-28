import pytest

import sys
from freezegun import freeze_time
try:
    from unittest import mock
except ImportError:
    import mock

import os
import shutil
import json
from collections import OrderedDict
from ruamel.yaml import YAML

from great_expectations.exceptions import DataContextError
from great_expectations.data_context import (
    ConfigOnlyDataContext,
    DataContext,
    ExplorerDataContext,
)
from great_expectations.data_context.util import safe_mmkdir
from great_expectations.data_context.types import (
    NormalizedDataAssetName,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.store import (
    BasicInMemoryStore,
    InMemoryEvaluationParameterStore,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

yaml = YAML()


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
    available_asset_names["my_datasource"]["default"] = set(available_asset_names["my_datasource"]["default"])

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
            DataAssetIdentifier(
                datasource='mydatasource',
                generator='mygenerator',
                generator_asset='source_diabetes_data'
            ): {
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
            DataAssetIdentifier(
                datasource='mydatasource',
                generator='mygenerator',
                generator_asset='source_patient_data'
            ): {
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
        data_context.normalize_data_asset_name("this/should/never/work/because/it/is/so/long")
        assert "found too many components using delimiter '/'" in exc.message


def test_normalize_data_asset_names_delimiters(empty_data_context, filesystem_csv):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv))
    data_context = empty_data_context

    data_context.data_asset_name_delimiter = '.'
    assert data_context.normalize_data_asset_name("my_datasource.default.f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    data_context.data_asset_name_delimiter = '/'
    assert data_context.normalize_data_asset_name("my_datasource/default/f1") == \
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
        empty_data_context.normalize_data_asset_name("f1")
        assert "No datasource configured" in exc.message

    with pytest.raises(DataContextError) as exc:
        empty_data_context.normalize_data_asset_name("my_datasource/f1")
        assert "No datasource configured" in exc.message

    with pytest.raises(DataContextError) as exc:
        empty_data_context.normalize_data_asset_name("my_datasource/default/f1")
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
    assert data_context.normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context.normalize_data_asset_name("my_datasource/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context.normalize_data_asset_name("my_datasource/default/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    # With only one datasource and generator configured, we
    # can create new namespaces at the generator asset level easily:
    assert data_context.normalize_data_asset_name("f5") == \
        NormalizedDataAssetName("my_datasource", "default", "f5")

    # We can also be more explicit in creating new namespaces at the generator asset level:
    assert data_context.normalize_data_asset_name("my_datasource/f6") == \
        NormalizedDataAssetName("my_datasource", "default", "f6")

    assert data_context.normalize_data_asset_name("my_datasource/default/f7") == \
        NormalizedDataAssetName("my_datasource", "default", "f7")

    # However, we cannot create against nonexisting datasources or generators:
    with pytest.raises(DataContextError) as exc:
        data_context.normalize_data_asset_name("my_fake_datasource/default/f7")
        assert "no configured datasource 'my_fake_datasource' with generator 'default'" in exc.message

    with pytest.raises(DataContextError) as exc:
        data_context.normalize_data_asset_name("my_datasource/my_fake_generator/f7")
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
    assert data_context.normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context.normalize_data_asset_name("f4") == \
        NormalizedDataAssetName("my_second_datasource", "default", "f4")

    # However, single-name resolution will fail with ambiguous entries
    with pytest.raises(DataContextError) as exc:
        data_context.normalize_data_asset_name("f3")
        assert "Ambiguous data_asset_name 'f3'. Multiple candidates found" in exc.message

    # Two-name resolution still works since generators are not ambiguous in that case
    assert data_context.normalize_data_asset_name("my_datasource/f3") == \
        NormalizedDataAssetName("my_datasource", "default", "f3")

    # We can also create new namespaces using only two components since that is not ambiguous
    assert data_context.normalize_data_asset_name("my_datasource/f9") == \
        NormalizedDataAssetName("my_datasource", "default", "f9")

    # However, we cannot create new names using only a single component
    with pytest.raises(DataContextError) as exc:
        data_context.normalize_data_asset_name("f10")
        assert "Ambiguous data_asset_name: no existing data_asset has the provided name" in exc.message

    ###
    # Add a second generator to one datasource
    ###
    my_datasource = data_context.get_datasource("my_datasource")
    my_datasource.add_generator("in_memory_generator", "memory")

    # We've chosen an interesting case: in_memory_generator does not by default provide its own names
    # so we can still get some names if there is no ambiguity about the namespace
    assert data_context.normalize_data_asset_name("f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    # However, if we add a data_asset that would cause that name to be ambiguous, it will then fail:
    suite = data_context.create_expectation_suite("my_datasource/in_memory_generator/f1", "default")
    data_context.save_expectation_suite(suite)

    with pytest.raises(DataContextError) as exc:
        name = data_context.normalize_data_asset_name("f1")
        assert "Ambiguous data_asset_name 'f1'. Multiple candidates found" in exc.message

    # It will also fail with two components since there is still ambiguity:
    with pytest.raises(DataContextError) as exc:
        data_context.normalize_data_asset_name("my_datasource/f1")
        assert "Ambiguous data_asset_name 'f1'. Multiple candidates found" in exc.message

    # But we can get the asset using all three components
    assert data_context.normalize_data_asset_name("my_datasource/default/f1") == \
        NormalizedDataAssetName("my_datasource", "default", "f1")

    assert data_context.normalize_data_asset_name("my_datasource/in_memory_generator/f1") == \
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
    profiling_results = titanic_data_context.profile_datasource("mydatasource")

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
        notebooks/
            create_expectations.ipynb
            integrate_validation_into_pipeline.ipynb
        plugins/
            custom_data_docs/
                renderers/
                styles/
                    data_docs_custom_styles.css
                views/
        uncommitted/
            config_variables.yml
            data_docs/
            samples/
            validations/
                profiling/
                    titanic/
                        default/
                            Titanic/
                                BasicDatasetProfiler.json
"""

    context.profile_datasource("random")
    context.build_data_docs()

    data_docs_dir = os.path.join(project_dir, "great_expectations/uncommitted/data_docs")
    observed = gen_directory_tree_str(data_docs_dir)
    print(observed)
    assert observed == """\
data_docs/
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

    # save data_docs locally
    safe_mmkdir("./tests/data_context/output")
    safe_mmkdir("./tests/data_context/output/data_docs")

    if os.path.isdir("./tests/data_context/output/data_docs"):
        shutil.rmtree("./tests/data_context/output/data_docs")
    shutil.copytree(
        os.path.join(
            ge_directory,
            "uncommitted/data_docs/"
        ),
        "./tests/data_context/output/data_docs"
    )


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
        "validations_store_name": "does_not_have_to_be_real",
        "expectations_store_name": "expectations_store",
        "config_variables_file_path": "uncommitted/config_variables.yml",
        "datasources": {},
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "FixedLengthTupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "evaluation_parameter_store" : {
                "module_name": "great_expectations.data_context.store",
                "class_name": "InMemoryEvaluationParameterStore",
            }
        },
        "data_docs_sites": {},
        "validation_operators": {
            "default": {
                "class_name": "ActionListValidationOperator",
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

    assert isinstance(context.evaluation_parameter_store, InMemoryEvaluationParameterStore)

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


def test_load_data_context_from_environment_variables(tmp_path_factory):
    try:
        project_path = str(tmp_path_factory.mktemp('data_context'))
        context_path = os.path.join(project_path, "great_expectations")
        safe_mmkdir(context_path)
        shutil.copy("./tests/test_fixtures/great_expectations_basic.yml",
                    str(os.path.join(context_path, "great_expectations.yml")))
        with pytest.raises(DataContextError) as err:
            DataContext.find_context_root_dir()
            assert "Unable to locate context root directory." in err

        os.environ["GE_HOME"] = context_path
        assert DataContext.find_context_root_dir() == context_path
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        del os.environ["GE_HOME"]


def test_data_context_updates_expectation_suite_names(data_context):
    # A data context should update the data_asset_name and expectation_suite_name of expectation suites
    # that it creates when it saves them.

    expectation_suites = data_context.list_expectation_suite_keys()

    # We should have a single expectation suite defined
    assert len(expectation_suites) == 1

    data_asset_name = expectation_suites[0]['data_asset_name']
    expectation_suite_name = expectation_suites[0]['expectation_suite_name']

    # We'll get that expectation suite and then update its name and re-save, then verify that everything
    # has been properly updated
    expectation_suite = data_context.get_expectation_suite(
        data_asset_name=data_asset_name,
        expectation_suite_name=expectation_suite_name
    )

    # Note we codify here the current behavior of having a string data_asset_name though typed ExpectationSuite objects
    # will enable changing that
    assert expectation_suite['data_asset_name'] == str(data_asset_name)
    assert expectation_suite['expectation_suite_name'] == expectation_suite_name

    # We will now change the data_asset_name and then save the suite in three ways:
    #   1. Directly using the new name,
    #   2. Using a different name that should be overwritten
    #   3. Using the new name but having the context draw that from the suite

    # Finally, we will try to save without a name (deleting it first) to demonstrate that saving will fail.
    expectation_suite['data_asset_name'] = str(DataAssetIdentifier(
        data_asset_name.datasource,
        data_asset_name.generator,
        "a_new_data_asset"
    ))
    expectation_suite['expectation_suite_name'] = 'a_new_suite_name'

    data_context.save_expectation_suite(
        expectation_suite=expectation_suite,
        data_asset_name=DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_data_asset"
        ),
        expectation_suite_name='a_new_suite_name'
    )

    fetched_expectation_suite = data_context.get_expectation_suite(
        data_asset_name=DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_data_asset"
        ),
        expectation_suite_name='a_new_suite_name'
    )

    assert fetched_expectation_suite['data_asset_name'] == str(
        DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_data_asset"
        )
    )
    assert fetched_expectation_suite['expectation_suite_name'] == 'a_new_suite_name'

    #   2. Using a different name that should be overwritten
    data_context.save_expectation_suite(
        expectation_suite=expectation_suite,
        data_asset_name=DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_new_data_asset"
        ),
        expectation_suite_name='a_new_new_suite_name'
    )

    fetched_expectation_suite = data_context.get_expectation_suite(
        data_asset_name=DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_new_data_asset"
        ),
        expectation_suite_name='a_new_new_suite_name'
    )

    assert fetched_expectation_suite['data_asset_name'] == str(
        DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_new_new_data_asset"
        )
    )
    assert fetched_expectation_suite['expectation_suite_name'] == 'a_new_new_suite_name'

    # Check that the saved name difference is actually persisted on disk
    with open(os.path.join(
                data_context.root_directory,
                "expectations",
                data_asset_name.datasource,
                data_asset_name.generator,
                "a_new_new_data_asset",
                "a_new_new_suite_name.json"
                ), 'r') as suite_file:
        loaded_suite = json.load(suite_file)
        assert loaded_suite['data_asset_name'] == str(
            DataAssetIdentifier(
                data_asset_name.datasource,
                data_asset_name.generator,
                "a_new_new_data_asset"
            )
        )
        assert loaded_suite['expectation_suite_name'] == 'a_new_new_suite_name'


    #   3. Using the new name but having the context draw that from the suite
    expectation_suite['data_asset_name'] = str(DataAssetIdentifier(
        data_asset_name.datasource,
        data_asset_name.generator,
        "a_third_name"
    ))
    expectation_suite['expectation_suite_name'] = "a_third_suite_name"
    data_context.save_expectation_suite(
        expectation_suite=expectation_suite
    )

    fetched_expectation_suite = data_context.get_expectation_suite(
        data_asset_name=DataAssetIdentifier(
            data_asset_name.datasource,
            data_asset_name.generator,
            "a_third_name"
        ),
        expectation_suite_name="a_third_suite_name"
    )
    assert fetched_expectation_suite['data_asset_name'] == str(DataAssetIdentifier(
        data_asset_name.datasource,
        data_asset_name.generator,
        "a_third_name"
    ))
    assert fetched_expectation_suite['expectation_suite_name'] == "a_third_suite_name"


def test_data_context_create_does_not_raise_error_or_warning_if_ge_dir_exists(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('data_context'))
    DataContext.create(project_path)


def test_data_context_create_raises_warning_and_leaves_existing_yml_untouched(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('data_context'))
    DataContext.create(project_path)
    ge_yml = os.path.join(
        project_path,
        "great_expectations/great_expectations.yml"
    )
    with open(ge_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    with pytest.warns(UserWarning):
        DataContext.create(project_path)

    with open(ge_yml, "r") as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


def test_data_context_create_makes_uncommitted_dirs_when_all_are_missing(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('data_context'))
    DataContext.create(project_path)

    # mangle the existing setup
    ge_dir = os.path.join(project_path, "great_expectations")
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    shutil.rmtree(uncommitted_dir)

    # re-run create to simulate onboarding
    DataContext.create(project_path)
    obs = gen_directory_tree_str(ge_dir)

    assert os.path.isdir(uncommitted_dir), "No uncommitted directory created"
    assert obs == """\
great_expectations/
    .gitignore
    great_expectations.yml
    datasources/
    expectations/
    notebooks/
        create_expectations.ipynb
        integrate_validation_into_pipeline.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        samples/
        validations/
"""


def test_data_context_create_does_nothing_if_all_uncommitted_dirs_exist(tmp_path_factory):
    expected = """\
great_expectations/
    .gitignore
    great_expectations.yml
    datasources/
    expectations/
    notebooks/
        create_expectations.ipynb
        integrate_validation_into_pipeline.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        samples/
        validations/
"""
    project_path = str(tmp_path_factory.mktemp('stuff'))
    ge_dir = os.path.join(project_path, "great_expectations")

    DataContext.create(project_path)
    fixture = gen_directory_tree_str(ge_dir)

    assert fixture == expected

    # re-run create to simulate onboarding
    DataContext.create(project_path)

    obs = gen_directory_tree_str(ge_dir)
    assert obs == expected


def test_data_context_do_all_uncommitted_dirs_exist(tmp_path_factory):
    expected = """\
uncommitted/
    config_variables.yml
    data_docs/
    samples/
    validations/
"""
    project_path = str(tmp_path_factory.mktemp('stuff'))
    ge_dir = os.path.join(project_path, "great_expectations")
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    DataContext.create(project_path)
    fixture = gen_directory_tree_str(uncommitted_dir)
    assert fixture == expected

    # Test that all exist
    assert DataContext.all_uncommitted_directories_exist(ge_dir)

    # remove a few
    shutil.rmtree(os.path.join(uncommitted_dir, "data_docs"))
    shutil.rmtree(os.path.join(uncommitted_dir, "validations"))

    # Test that not all exist
    assert not DataContext.all_uncommitted_directories_exist(project_path)


def test_data_context_create_does_not_overwrite_existing_config_variables_yml(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('data_context'))
    DataContext.create(project_path)
    ge_dir = os.path.join(project_path, "great_expectations")
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    config_vars_yml = os.path.join(uncommitted_dir, "config_variables.yml")

    # modify config variables
    with open(config_vars_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    # re-run create to simulate onboarding
    with pytest.warns(UserWarning):
        DataContext.create(project_path)

    with open(config_vars_yml, "r") as ff:
        obs = ff.read()
    print(obs)
    assert "# LOOK I WAS MODIFIED" in obs


def test_scaffold_directories_and_notebooks(tmp_path_factory):
    empty_directory = str(tmp_path_factory.mktemp("test_scaffold_directories_and_notebooks"))
    DataContext.scaffold_directories(empty_directory)
    DataContext.scaffold_notebooks(empty_directory)

    assert set(os.listdir(empty_directory)) == {
        'datasources',
        'plugins',
        'expectations',
        '.gitignore',
        'uncommitted',
        'notebooks'
    }
    assert set(os.listdir(os.path.join(empty_directory, "uncommitted"))) == {
        'samples',
        'data_docs',
        'validations'
    }
    assert set(os.listdir(os.path.join(empty_directory, "notebooks"))) == {
        "create_expectations.ipynb",
        "integrate_validation_into_pipeline.ipynb"
    }


def test_build_batch_kwargs(titanic_multibatch_data_context):
    data_asset_name = titanic_multibatch_data_context.normalize_data_asset_name("titanic")
    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(data_asset_name, "Titanic_1911")
    assert "./data/titanic/Titanic_1911.csv" in batch_kwargs["path"]
    assert "partition_id" in batch_kwargs
    assert batch_kwargs["partition_id"] == "Titanic_1911"


def test_load_config_variables_file(basic_data_context_config, tmp_path_factory):
    # Setup:
    base_path = str(tmp_path_factory.mktemp('test_load_config_variables_file'))
    safe_mmkdir(os.path.join(base_path, "uncommitted"))
    with open(os.path.join(base_path, "uncommitted", "dev_variables.yml"), "w") as outfile:
        yaml.dump({'env': 'dev'}, outfile)
    with open(os.path.join(base_path, "uncommitted", "prod_variables.yml"), "w") as outfile:
        yaml.dump({'env': 'prod'}, outfile)
    basic_data_context_config["config_variables_file_path"] = "uncommitted/${TEST_CONFIG_FILE_ENV}_variables.yml"
    context = ConfigOnlyDataContext(basic_data_context_config, context_root_dir=base_path)

    try:
        # We should be able to load different files based on an environment variable
        os.environ["TEST_CONFIG_FILE_ENV"] = "dev"
        vars = context._load_config_variables_file()
        assert vars['env'] == 'dev'
        os.environ["TEST_CONFIG_FILE_ENV"] = "prod"
        vars = context._load_config_variables_file()
        assert vars['env'] == 'prod'
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        del os.environ["TEST_CONFIG_FILE_ENV"]
