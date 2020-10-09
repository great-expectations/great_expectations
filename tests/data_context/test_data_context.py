import json
import os
import shutil

import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuite,
    RunIdentifier,
    expectationSuiteSchema,
)
from great_expectations.data_context import (
    BaseDataContext,
    DataContext,
    ExplorerDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset import Dataset
from great_expectations.datasource import Datasource
from great_expectations.datasource.types.batch_kwargs import PathBatchKwargs
from great_expectations.exceptions import (
    BatchKwargsError,
    CheckpointError,
    CheckpointNotFoundError,
    ConfigNotFoundError,
    DataContextError,
)
from great_expectations.util import gen_directory_tree_str
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)
from tests.test_utils import safe_remove

try:
    from unittest import mock
except ImportError:
    from unittest import mock

yaml = YAML()


@pytest.fixture()
def parameterized_expectation_suite():
    fixture_path = file_relative_path(
        __file__,
        "../test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json",
    )
    with open(fixture_path,) as suite:
        return json.load(suite)


def test_create_duplicate_expectation_suite(titanic_data_context):
    # create new expectation suite
    assert titanic_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite"
    )
    # attempt to create expectation suite with name that already exists on data asset
    with pytest.raises(DataContextError):
        titanic_data_context.create_expectation_suite(
            expectation_suite_name="titanic.test_create_expectation_suite"
        )
    # create expectation suite with name that already exists on data asset, but pass overwrite_existing=True
    assert titanic_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite",
        overwrite_existing=True,
    )


def test_get_available_data_asset_names_with_one_datasource_including_a_single_generator(
    empty_data_context, filesystem_csv
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": str(filesystem_csv),
            }
        },
    )

    available_asset_names = empty_data_context.get_available_data_asset_names()

    assert set(available_asset_names["my_datasource"]["subdir_reader"]["names"]) == {
        ("f3", "directory"),
        ("f2", "file"),
        ("f1", "file"),
    }


def test_get_available_data_asset_names_with_one_datasource_without_a_generator_returns_empty_dict(
    empty_data_context,
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    obs = empty_data_context.get_available_data_asset_names()
    assert obs == {"my_datasource": {}}


def test_get_available_data_asset_names_with_multiple_datasources_with_and_without_generators(
    empty_data_context, sa
):
    """Test datasources with and without generators."""
    # requires sqlalchemy because it instantiates sqlalchemydatasource
    context = empty_data_context
    connection_kwargs = {"credentials": {"drivername": "sqlite"}}

    context.add_datasource(
        "first",
        class_name="SqlAlchemyDatasource",
        batch_kwargs_generators={"foo": {"class_name": "TableBatchKwargsGenerator",}},
        **connection_kwargs,
    )
    context.add_datasource(
        "second", class_name="SqlAlchemyDatasource", **connection_kwargs
    )
    context.add_datasource(
        "third",
        class_name="SqlAlchemyDatasource",
        batch_kwargs_generators={"bar": {"class_name": "TableBatchKwargsGenerator",}},
        **connection_kwargs,
    )

    obs = context.get_available_data_asset_names()

    assert isinstance(obs, dict)
    assert set(obs.keys()) == {"first", "second", "third"}
    assert obs == {
        "first": {"foo": {"is_complete_list": True, "names": []}},
        "second": {},
        "third": {"bar": {"is_complete_list": True, "names": []}},
    }


def test_list_expectation_suite_keys(data_context_parameterized_expectation_suite):
    assert data_context_parameterized_expectation_suite.list_expectation_suites() == [
        ExpectationSuiteIdentifier(expectation_suite_name="my_dag_node.default")
    ]


def test_get_existing_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "my_dag_node.default"
    )
    assert expectation_suite.expectation_suite_name == "my_dag_node.default"
    assert len(expectation_suite.expectations) == 2


def test_get_new_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.create_expectation_suite(
        "this_data_asset_does_not_exist.default"
    )
    assert (
        expectation_suite.expectation_suite_name
        == "this_data_asset_does_not_exist.default"
    )
    assert len(expectation_suite.expectations) == 0


def test_save_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.create_expectation_suite(
        "this_data_asset_config_does_not_exist.default"
    )
    expectation_suite.expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite
    )
    expectation_suite_saved = data_context_parameterized_expectation_suite.get_expectation_suite(
        "this_data_asset_config_does_not_exist.default"
    )
    assert expectation_suite.expectations == expectation_suite_saved.expectations


def test_compile_evaluation_parameter_dependencies(
    data_context_parameterized_expectation_suite,
):
    assert (
        data_context_parameterized_expectation_suite._evaluation_parameter_dependencies
        == {}
    )
    data_context_parameterized_expectation_suite._compile_evaluation_parameter_dependencies()
    assert (
        data_context_parameterized_expectation_suite._evaluation_parameter_dependencies
        == {
            "source_diabetes_data.default": [
                {
                    "metric_kwargs_id": {
                        "column=patient_nbr": [
                            "expect_column_unique_value_count_to_be_between.result.observed_value"
                        ]
                    }
                }
            ],
            "source_patient_data.default": [
                "expect_table_row_count_to_equal.result.observed_value"
            ],
        }
    )


def test_list_datasources(data_context_parameterized_expectation_suite):
    datasources = data_context_parameterized_expectation_suite.list_datasources()

    assert datasources == [
        {
            "name": "mydatasource",
            "class_name": "PandasDatasource",
            "module_name": "great_expectations.datasource",
            "data_asset_type": {"class_name": "PandasDataset"},
            "batch_kwargs_generators": {
                "mygenerator": {
                    "base_directory": "../data",
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "reader_options": {"engine": "python", "sep": None},
                }
            },
        }
    ]

    data_context_parameterized_expectation_suite.add_datasource(
        "second_pandas_source",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    datasources = data_context_parameterized_expectation_suite.list_datasources()

    assert datasources == [
        {
            "name": "mydatasource",
            "class_name": "PandasDatasource",
            "module_name": "great_expectations.datasource",
            "data_asset_type": {"class_name": "PandasDataset"},
            "batch_kwargs_generators": {
                "mygenerator": {
                    "base_directory": "../data",
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "reader_options": {"engine": "python", "sep": None},
                }
            },
        },
        {
            "name": "second_pandas_source",
            "class_name": "PandasDatasource",
            "module_name": "great_expectations.datasource",
            "data_asset_type": {
                "class_name": "PandasDataset",
                "module_name": "great_expectations.dataset",
            },
        },
    ]


@freeze_time("09/26/2019 13:42:41")
def test_data_context_get_validation_result(titanic_data_context):
    """
    Test that validation results can be correctly fetched from the configured results store
    """
    run_id = RunIdentifier(run_name="profiling")
    profiling_results = titanic_data_context.profile_datasource(
        "mydatasource", run_id=run_id
    )

    all_validation_result = titanic_data_context.get_validation_result(
        "mydatasource.mygenerator.Titanic.BasicDatasetProfiler", run_id=run_id
    )
    assert len(all_validation_result.results) == 51

    failed_validation_result = titanic_data_context.get_validation_result(
        "mydatasource.mygenerator.Titanic.BasicDatasetProfiler",
        run_id=run_id,
        failed_only=True,
    )
    assert len(failed_validation_result.results) == 8


def test_data_context_get_datasource(titanic_data_context):
    isinstance(titanic_data_context.get_datasource("mydatasource"), Datasource)


def test_data_context_expectation_suite_delete(empty_data_context):
    assert empty_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 1
    empty_data_context.delete_expectation_suite(
        expectation_suite_name=expectation_suites[0]
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 0


def test_data_context_expectation_nested_suite_delete(empty_data_context):
    assert empty_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test.create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert empty_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test.a.create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 2
    empty_data_context.delete_expectation_suite(
        expectation_suite_name=expectation_suites[0]
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 1


def test_data_context_get_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context,
):
    with pytest.raises(ValueError):
        _ = titanic_data_context.get_datasource("fakey_mc_fake")


def test_data_context_profile_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context,
):
    with pytest.raises(ValueError):
        _ = titanic_data_context.profile_datasource("fakey_mc_fake")


@freeze_time("09/26/2019 13:42:41")
@pytest.mark.rendered_output
def test_render_full_static_site_from_empty_project(tmp_path_factory, filesystem_csv_3):

    # TODO : Use a standard test fixture
    # TODO : Have that test fixture copy a directory, rather than building a new one from scratch

    base_dir = str(tmp_path_factory.mktemp("project_dir"))
    project_dir = os.path.join(base_dir, "project_path")
    os.mkdir(project_dir)

    os.makedirs(os.path.join(project_dir, "data"))
    os.makedirs(os.path.join(project_dir, "data/titanic"))
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(project_dir, "data/titanic/Titanic.csv")),
    )

    os.makedirs(os.path.join(project_dir, "data/random"))
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),
        str(os.path.join(project_dir, "data/random/f1.csv")),
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),
        str(os.path.join(project_dir, "data/random/f2.csv")),
    )

    assert (
        gen_directory_tree_str(project_dir)
        == """\
project_path/
    data/
        random/
            f1.csv
            f2.csv
        titanic/
            Titanic.csv
"""
    )

    context = DataContext.create(project_dir)
    ge_directory = os.path.join(project_dir, "great_expectations")
    context.add_datasource(
        "titanic",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data/titanic/"),
            }
        },
    )

    context.add_datasource(
        "random",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(project_dir, "data/random/"),
            }
        },
    )

    context.profile_datasource("titanic")

    # Replicate the batch id of the batch that will be profiled in order to generate the file path of the
    # validation result
    titanic_profiled_batch_id = PathBatchKwargs(
        {
            "path": os.path.join(project_dir, "data/titanic/Titanic.csv"),
            "datasource": "titanic",
            "data_asset_name": "Titanic",
        }
    ).to_id()

    tree_str = gen_directory_tree_str(project_dir)
    # print(tree_str)
    assert (
        tree_str
        == """project_path/
    data/
        random/
            f1.csv
            f2.csv
        titanic/
            Titanic.csv
    great_expectations/
        .gitignore
        great_expectations.yml
        checkpoints/
        expectations/
            titanic/
                subdir_reader/
                    Titanic/
                        BasicDatasetProfiler.json
        notebooks/
            pandas/
                validation_playground.ipynb
            spark/
                validation_playground.ipynb
            sql/
                validation_playground.ipynb
        plugins/
            custom_data_docs/
                renderers/
                styles/
                    data_docs_custom_styles.css
                views/
        uncommitted/
            config_variables.yml
            data_docs/
            validations/
                titanic/
                    subdir_reader/
                        Titanic/
                            BasicDatasetProfiler/
                                profiling/
                                    20190926T134241.000000Z/
                                        {}.json
""".format(
            titanic_profiled_batch_id
        )
    )

    context.profile_datasource("random")
    context.build_data_docs()

    f1_profiled_batch_id = PathBatchKwargs(
        {
            "path": os.path.join(project_dir, "data/random/f1.csv"),
            "datasource": "random",
            "data_asset_name": "f1",
        }
    ).to_id()

    f2_profiled_batch_id = PathBatchKwargs(
        {
            "path": os.path.join(project_dir, "data/random/f2.csv"),
            "datasource": "random",
            "data_asset_name": "f2",
        }
    ).to_id()

    data_docs_dir = os.path.join(
        project_dir, "great_expectations/uncommitted/data_docs"
    )
    observed = gen_directory_tree_str(data_docs_dir)
    print(observed)
    assert (
        observed
        == """\
data_docs/
    local_site/
        index.html
        expectations/
            random/
                subdir_reader/
                    f1/
                        BasicDatasetProfiler.html
                    f2/
                        BasicDatasetProfiler.html
            titanic/
                subdir_reader/
                    Titanic/
                        BasicDatasetProfiler.html
        static/
            fonts/
                HKGrotesk/
                    HKGrotesk-Bold.otf
                    HKGrotesk-BoldItalic.otf
                    HKGrotesk-Italic.otf
                    HKGrotesk-Light.otf
                    HKGrotesk-LightItalic.otf
                    HKGrotesk-Medium.otf
                    HKGrotesk-MediumItalic.otf
                    HKGrotesk-Regular.otf
                    HKGrotesk-SemiBold.otf
                    HKGrotesk-SemiBoldItalic.otf
            images/
                favicon.ico
                glossary_scroller.gif
                iterative-dev-loop.png
                logo-long-vector.svg
                logo-long.png
                short-logo-vector.svg
                short-logo.png
                validation_failed_unexpected_values.gif
            styles/
                data_docs_custom_styles_template.css
                data_docs_default_styles.css
        validations/
            random/
                subdir_reader/
                    f1/
                        BasicDatasetProfiler/
                            profiling/
                                20190926T134241.000000Z/
                                    {:s}.html
                    f2/
                        BasicDatasetProfiler/
                            profiling/
                                20190926T134241.000000Z/
                                    {:s}.html
            titanic/
                subdir_reader/
                    Titanic/
                        BasicDatasetProfiler/
                            profiling/
                                20190926T134241.000000Z/
                                    {:s}.html
""".format(
            f1_profiled_batch_id, f2_profiled_batch_id, titanic_profiled_batch_id
        )
    )

    # save data_docs locally
    os.makedirs("./tests/data_context/output", exist_ok=True)
    os.makedirs("./tests/data_context/output/data_docs", exist_ok=True)

    if os.path.isdir("./tests/data_context/output/data_docs"):
        shutil.rmtree("./tests/data_context/output/data_docs")
    shutil.copytree(
        os.path.join(ge_directory, "uncommitted/data_docs/"),
        "./tests/data_context/output/data_docs",
    )


def test_add_store(empty_data_context):
    assert "my_new_store" not in empty_data_context.stores.keys()
    assert "my_new_store" not in empty_data_context.get_config()["stores"]
    new_store = empty_data_context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )
    assert "my_new_store" in empty_data_context.stores.keys()
    assert "my_new_store" in empty_data_context.get_config()["stores"]

    assert isinstance(new_store, ExpectationsStore)


@pytest.fixture
def basic_data_context_config():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
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
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/",
                    },
                },
                "evaluation_parameter_store": {
                    "module_name": "great_expectations.data_context.store",
                    "class_name": "EvaluationParameterStore",
                },
            },
            "data_docs_sites": {},
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
        }
    )


def test_ExplorerDataContext(titanic_data_context):
    context_root_directory = titanic_data_context.root_directory
    explorer_data_context = ExplorerDataContext(context_root_directory)
    assert explorer_data_context._expectation_explorer_manager


def test_ConfigOnlyDataContext__initialization(
    tmp_path_factory, basic_data_context_config
):
    config_path = str(
        tmp_path_factory.mktemp("test_ConfigOnlyDataContext__initialization__dir")
    )
    context = BaseDataContext(basic_data_context_config, config_path,)

    assert (
        context.root_directory.split("/")[-1]
        == "test_ConfigOnlyDataContext__initialization__dir0"
    )
    assert context.plugins_directory.split("/")[-3:] == [
        "test_ConfigOnlyDataContext__initialization__dir0",
        "plugins",
        "",
    ]


def test__normalize_absolute_or_relative_path(
    tmp_path_factory, basic_data_context_config
):
    config_path = str(
        tmp_path_factory.mktemp("test__normalize_absolute_or_relative_path__dir")
    )
    context = BaseDataContext(basic_data_context_config, config_path,)

    assert str(
        os.path.join("test__normalize_absolute_or_relative_path__dir0", "yikes")
    ) in context._normalize_absolute_or_relative_path("yikes")

    assert (
        "test__normalize_absolute_or_relative_path__dir"
        not in context._normalize_absolute_or_relative_path("/yikes")
    )
    assert "/yikes" == context._normalize_absolute_or_relative_path("/yikes")


def test_load_data_context_from_environment_variables(tmp_path_factory):
    curdir = os.path.abspath(os.getcwd())
    try:
        project_path = str(tmp_path_factory.mktemp("data_context"))
        context_path = os.path.join(project_path, "great_expectations")
        os.makedirs(context_path, exist_ok=True)
        os.chdir(context_path)
        with pytest.raises(DataContextError) as err:
            DataContext.find_context_root_dir()
        assert isinstance(err.value, ConfigNotFoundError)

        shutil.copy(
            file_relative_path(
                __file__, "../test_fixtures/great_expectations_basic.yml"
            ),
            str(os.path.join(context_path, "great_expectations.yml")),
        )
        os.environ["GE_HOME"] = context_path
        assert DataContext.find_context_root_dir() == context_path
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        if "GE_HOME" in os.environ:
            del os.environ["GE_HOME"]
        os.chdir(curdir)


def test_data_context_updates_expectation_suite_names(
    data_context_parameterized_expectation_suite,
):
    # A data context should update the data_asset_name and expectation_suite_name of expectation suites
    # that it creates when it saves them.

    expectation_suites = (
        data_context_parameterized_expectation_suite.list_expectation_suites()
    )

    # We should have a single expectation suite defined
    assert len(expectation_suites) == 1

    expectation_suite_name = expectation_suites[0].expectation_suite_name

    # We'll get that expectation suite and then update its name and re-save, then verify that everything
    # has been properly updated
    expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        expectation_suite_name
    )

    # Note we codify here the current behavior of having a string data_asset_name though typed ExpectationSuite objects
    # will enable changing that
    assert expectation_suite.expectation_suite_name == expectation_suite_name

    # We will now change the data_asset_name and then save the suite in three ways:
    #   1. Directly using the new name,
    #   2. Using a different name that should be overwritten
    #   3. Using the new name but having the context draw that from the suite

    # Finally, we will try to save without a name (deleting it first) to demonstrate that saving will fail.

    expectation_suite.expectation_suite_name = "a_new_suite_name"

    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite, expectation_suite_name="a_new_suite_name"
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_new_suite_name"
    )

    assert fetched_expectation_suite.expectation_suite_name == "a_new_suite_name"

    #   2. Using a different name that should be overwritten
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite,
        expectation_suite_name="a_new_new_suite_name",
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_new_new_suite_name"
    )

    assert fetched_expectation_suite.expectation_suite_name == "a_new_new_suite_name"

    # Check that the saved name difference is actually persisted on disk
    with open(
        os.path.join(
            data_context_parameterized_expectation_suite.root_directory,
            "expectations",
            "a_new_new_suite_name.json",
        ),
    ) as suite_file:
        loaded_suite = expectationSuiteSchema.load(json.load(suite_file))
        assert loaded_suite.expectation_suite_name == "a_new_new_suite_name"

    #   3. Using the new name but having the context draw that from the suite
    expectation_suite.expectation_suite_name = "a_third_suite_name"
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_third_suite_name"
    )
    assert fetched_expectation_suite.expectation_suite_name == "a_third_suite_name"


def test_data_context_create_does_not_raise_error_or_warning_if_ge_dir_exists(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    DataContext.create(project_path)


@pytest.fixture()
def empty_context(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    DataContext.create(project_path)
    ge_dir = os.path.join(project_path, "great_expectations")
    assert os.path.isdir(ge_dir)
    assert os.path.isfile(os.path.join(ge_dir, DataContext.GE_YML))
    context = DataContext(ge_dir)
    assert isinstance(context, DataContext)
    return context


def test_data_context_does_ge_yml_exist_returns_true_when_it_does_exist(empty_context):
    ge_dir = empty_context.root_directory
    assert DataContext.does_config_exist_on_disk(ge_dir) == True


def test_data_context_does_ge_yml_exist_returns_false_when_it_does_not_exist(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(os.path.join(ge_dir, empty_context.GE_YML))
    assert DataContext.does_config_exist_on_disk(ge_dir) == False


def test_data_context_does_project_have_a_datasource_in_config_file_returns_true_when_it_has_a_datasource_configured_in_yml_file_on_disk(
    empty_context,
):
    ge_dir = empty_context.root_directory
    empty_context.add_datasource("arthur", **{"class_name": "PandasDatasource"})
    assert DataContext.does_project_have_a_datasource_in_config_file(ge_dir) == True


def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_datasource_configured_in_yml_file_on_disk(
    empty_context,
):
    ge_dir = empty_context.root_directory
    assert DataContext.does_project_have_a_datasource_in_config_file(ge_dir) == False


def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_yml_file(
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir, empty_context.GE_YML))
    assert DataContext.does_project_have_a_datasource_in_config_file(ge_dir) == False


def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_dir(
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir))
    assert DataContext.does_project_have_a_datasource_in_config_file(ge_dir) == False


def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_the_project_has_an_invalid_config_file(
    empty_context,
):
    ge_dir = empty_context.root_directory
    with open(os.path.join(ge_dir, DataContext.GE_YML), "w") as yml:
        yml.write("this file: is not a valid ge config")
    assert DataContext.does_project_have_a_datasource_in_config_file(ge_dir) == False


def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_one_suite(
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.add_datasource("arthur", class_name="PandasDatasource")
    context.create_expectation_suite("dent")
    assert len(context.list_expectation_suites()) == 1

    assert DataContext.is_project_initialized(ge_dir) == True


def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_no_suites(
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.add_datasource("arthur", class_name="PandasDatasource")
    assert len(context.list_expectation_suites()) == 0

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_its_valid_context_has_no_datasource(
    empty_context,
):
    ge_dir = empty_context.root_directory
    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_config_yml_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(os.path.join(ge_dir, empty_context.GE_YML))

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_uncommitted_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(os.path.join(ge_dir, empty_context.GE_UNCOMMITTED_DIR))

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_uncommitted_data_docs_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(os.path.join(ge_dir, empty_context.GE_UNCOMMITTED_DIR, "data_docs"))

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_uncommitted_validations_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(os.path.join(ge_dir, empty_context.GE_UNCOMMITTED_DIR, "validations"))

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_is_project_initialized_returns_false_when_config_variable_yml_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(
        os.path.join(ge_dir, empty_context.GE_UNCOMMITTED_DIR, "config_variables.yml")
    )

    assert DataContext.is_project_initialized(ge_dir) == False


def test_data_context_create_raises_warning_and_leaves_existing_yml_untouched(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    DataContext.create(project_path)
    ge_yml = os.path.join(project_path, "great_expectations/great_expectations.yml")
    with open(ge_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    with pytest.warns(UserWarning):
        DataContext.create(project_path)

    with open(ge_yml) as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


def test_data_context_create_makes_uncommitted_dirs_when_all_are_missing(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    DataContext.create(project_path)

    # mangle the existing setup
    ge_dir = os.path.join(project_path, "great_expectations")
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    shutil.rmtree(uncommitted_dir)

    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        # re-run create to simulate onboarding
        DataContext.create(project_path)
    obs = gen_directory_tree_str(ge_dir)

    assert os.path.isdir(uncommitted_dir), "No uncommitted directory created"
    assert (
        obs
        == """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
    expectations/
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
"""
    )


def test_data_context_create_does_nothing_if_all_uncommitted_dirs_exist(
    tmp_path_factory,
):
    expected = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
    expectations/
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
"""
    project_path = str(tmp_path_factory.mktemp("stuff"))
    ge_dir = os.path.join(project_path, "great_expectations")

    DataContext.create(project_path)
    fixture = gen_directory_tree_str(ge_dir)

    assert fixture == expected

    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        # re-run create to simulate onboarding
        DataContext.create(project_path)

    obs = gen_directory_tree_str(ge_dir)
    assert obs == expected


def test_data_context_do_all_uncommitted_dirs_exist(tmp_path_factory):
    expected = """\
uncommitted/
    config_variables.yml
    data_docs/
    validations/
"""
    project_path = str(tmp_path_factory.mktemp("stuff"))
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


def test_data_context_create_builds_base_directories(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context = DataContext.create(project_path)
    assert isinstance(context, DataContext)

    for directory in [
        "expectations",
        "notebooks",
        "plugins",
        "checkpoints",
        "uncommitted",
    ]:
        base_dir = os.path.join(project_path, context.GE_DIR, directory)
        assert os.path.isdir(base_dir)


def test_data_context_create_does_not_overwrite_existing_config_variables_yml(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
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

    with open(config_vars_yml) as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


def test_scaffold_directories_and_notebooks(tmp_path_factory):
    empty_directory = str(
        tmp_path_factory.mktemp("test_scaffold_directories_and_notebooks")
    )
    DataContext.scaffold_directories(empty_directory)
    DataContext.scaffold_notebooks(empty_directory)

    assert set(os.listdir(empty_directory)) == {
        "plugins",
        "checkpoints",
        "expectations",
        ".gitignore",
        "uncommitted",
        "notebooks",
    }
    assert set(os.listdir(os.path.join(empty_directory, "uncommitted"))) == {
        "data_docs",
        "validations",
    }
    for subdir in DataContext.NOTEBOOK_SUBDIRECTORIES:
        subdir_path = os.path.join(empty_directory, "notebooks", subdir)
        assert set(os.listdir(subdir_path)) == {"validation_playground.ipynb"}


def test_build_batch_kwargs(titanic_multibatch_data_context):
    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(
        "mydatasource",
        "mygenerator",
        data_asset_name="titanic",
        partition_id="Titanic_1912",
    )
    assert os.path.relpath("./data/titanic/Titanic_1912.csv") in batch_kwargs["path"]

    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(
        "mydatasource",
        "mygenerator",
        data_asset_name="titanic",
        partition_id="Titanic_1911",
    )
    assert os.path.relpath("./data/titanic/Titanic_1911.csv") in batch_kwargs["path"]

    paths = []
    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(
        "mydatasource", "mygenerator", data_asset_name="titanic"
    )
    paths.append(os.path.basename(batch_kwargs["path"]))

    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(
        "mydatasource", "mygenerator", data_asset_name="titanic"
    )
    paths.append(os.path.basename(batch_kwargs["path"]))

    assert {"Titanic_1912.csv", "Titanic_1911.csv"} == set(paths)


def test_existing_local_data_docs_urls_returns_url_on_project_with_no_datasources_and_a_site_configured(
    tmp_path_factory,
):
    """
    This test ensures that a url will be returned for a default site even if a
    datasource is not configured, and docs are not built.
    """
    empty_directory = str(tmp_path_factory.mktemp("another_empty_project"))
    DataContext.create(empty_directory)
    context = DataContext(os.path.join(empty_directory, DataContext.GE_DIR))

    obs = context.get_docs_sites_urls(only_if_exists=False)
    assert len(obs) == 1
    assert obs[0]["site_url"].endswith(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )


def test_existing_local_data_docs_urls_returns_single_url_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo"))
    DataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, DataContext.GE_DIR)
    context = DataContext(ge_dir)

    context._project_config["data_docs_sites"] = {
        "my_rad_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/some/local/path/",
            },
        }
    }

    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    context = DataContext(ge_dir)
    context.build_data_docs()

    expected_path = os.path.join(
        ge_dir, "uncommitted/data_docs/some/local/path/index.html"
    )
    assert os.path.isfile(expected_path)

    obs = context.get_docs_sites_urls()
    assert obs == [
        {"site_name": "my_rad_site", "site_url": "file://{}".format(expected_path)}
    ]


def test_existing_local_data_docs_urls_returns_multiple_urls_from_customized_local_site(
    tmp_path_factory,
):
    empty_directory = str(tmp_path_factory.mktemp("yo_yo_ma"))
    DataContext.create(empty_directory)
    ge_dir = os.path.join(empty_directory, DataContext.GE_DIR)
    context = DataContext(ge_dir)

    context._project_config["data_docs_sites"] = {
        "my_rad_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/some/path/",
            },
        },
        "another_just_amazing_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/another/path/",
            },
        },
    }

    # TODO Workaround project config programmatic config manipulation
    #  statefulness issues by writing to disk and re-upping a new context
    context._save_project_config()
    context = DataContext(ge_dir)
    context.build_data_docs()
    data_docs_dir = os.path.join(ge_dir, "uncommitted/data_docs/")

    path_1 = os.path.join(data_docs_dir, "some/path/index.html")
    path_2 = os.path.join(data_docs_dir, "another/path/index.html")
    for expected_path in [path_1, path_2]:
        assert os.path.isfile(expected_path)

    obs = context.get_docs_sites_urls()

    assert obs == [
        {"site_name": "my_rad_site", "site_url": "file://{}".format(path_1)},
        {
            "site_name": "another_just_amazing_site",
            "site_url": "file://{}".format(path_2),
        },
    ]


def test_load_config_variables_file(basic_data_context_config, tmp_path_factory):
    # Setup:
    base_path = str(tmp_path_factory.mktemp("test_load_config_variables_file"))
    os.makedirs(os.path.join(base_path, "uncommitted"), exist_ok=True)
    with open(
        os.path.join(base_path, "uncommitted", "dev_variables.yml"), "w"
    ) as outfile:
        yaml.dump({"env": "dev"}, outfile)
    with open(
        os.path.join(base_path, "uncommitted", "prod_variables.yml"), "w"
    ) as outfile:
        yaml.dump({"env": "prod"}, outfile)
    basic_data_context_config[
        "config_variables_file_path"
    ] = "uncommitted/${TEST_CONFIG_FILE_ENV}_variables.yml"

    try:
        # We should be able to load different files based on an environment variable
        os.environ["TEST_CONFIG_FILE_ENV"] = "dev"
        context = BaseDataContext(basic_data_context_config, context_root_dir=base_path)
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "dev"
        os.environ["TEST_CONFIG_FILE_ENV"] = "prod"
        context = BaseDataContext(basic_data_context_config, context_root_dir=base_path)
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "prod"
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        del os.environ["TEST_CONFIG_FILE_ENV"]


def test_list_expectation_suite_with_no_suites(titanic_data_context):
    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == []


def test_list_expectation_suite_with_one_suite(titanic_data_context):
    titanic_data_context.create_expectation_suite("warning")
    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == ["warning"]


def test_list_expectation_suite_with_multiple_suites(titanic_data_context):
    titanic_data_context.create_expectation_suite("a.warning")
    titanic_data_context.create_expectation_suite("b.warning")
    titanic_data_context.create_expectation_suite("c.warning")

    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == ["a.warning", "b.warning", "c.warning"]
    assert len(observed) == 3


def test_get_batch_raises_error_when_passed_a_non_string_type_for_suite_parameter(
    titanic_data_context,
):
    with pytest.raises(DataContextError):
        titanic_data_context.get_batch({}, 99)


def test_get_batch_raises_error_when_passed_a_non_dict_or_batch_kwarg_type_for_batch_kwarg_parameter(
    titanic_data_context,
):
    with pytest.raises(BatchKwargsError):
        titanic_data_context.get_batch(99, "foo")


def test_get_batch_when_passed_a_suite_name(titanic_data_context):
    context = titanic_data_context
    root_dir = context.root_directory
    batch_kwargs = {
        "datasource": "mydatasource",
        "path": os.path.join(root_dir, "..", "data", "Titanic.csv"),
    }
    context.create_expectation_suite("foo")
    assert context.list_expectation_suite_names() == ["foo"]
    batch = context.get_batch(batch_kwargs, "foo")
    assert isinstance(batch, Dataset)
    assert isinstance(batch.get_expectation_suite(), ExpectationSuite)


def test_get_batch_when_passed_a_suite(titanic_data_context):
    context = titanic_data_context
    root_dir = context.root_directory
    batch_kwargs = {
        "datasource": "mydatasource",
        "path": os.path.join(root_dir, "..", "data", "Titanic.csv"),
    }
    context.create_expectation_suite("foo")
    assert context.list_expectation_suite_names() == ["foo"]
    suite = context.get_expectation_suite("foo")

    batch = context.get_batch(batch_kwargs, suite)
    assert isinstance(batch, Dataset)
    assert isinstance(batch.get_expectation_suite(), ExpectationSuite)


def test_list_validation_operators_data_context_with_none_returns_empty_list(
    titanic_data_context,
):
    titanic_data_context.validation_operators = {}
    assert titanic_data_context.list_validation_operator_names() == []


def test_list_validation_operators_data_context_with_one(titanic_data_context):
    assert titanic_data_context.list_validation_operator_names() == [
        "action_list_operator"
    ]


def test_list_checkpoints_on_empty_context_returns_empty_list(empty_data_context):
    assert empty_data_context.list_checkpoints() == []


def test_list_checkpoints_on_context_with_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    assert context.list_checkpoints() == ["my_checkpoint"]


def test_list_checkpoints_on_context_with_twwo_checkpoints(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    checkpoints_file = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "my_checkpoint.yml"
    )
    shutil.copy(
        checkpoints_file, os.path.join(os.path.dirname(checkpoints_file), "another.yml")
    )
    assert set(context.list_checkpoints()) == {"another", "my_checkpoint"}


def test_list_checkpoints_on_context_with_checkpoint_and_other_files_in_checkpoints_dir(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint

    for extension in [".json", ".txt", "", ".py"]:
        path = os.path.join(
            context.root_directory, context.CHECKPOINTS_DIR, f"foo{extension}"
        )
        with open(path, "w") as f:
            f.write("foo: bar")
        assert os.path.isfile(path)

    assert context.list_checkpoints() == ["my_checkpoint"]


def test_get_checkpoint_raises_error_on_not_found_checkpoint(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    with pytest.raises(CheckpointNotFoundError):
        context.get_checkpoint("not_a_checkpoint")


def test_get_checkpoint_raises_error_empty_checkpoint(empty_context_with_checkpoint,):
    context = empty_context_with_checkpoint
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "my_checkpoint.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        f.write("# Not a checkpoint file")
    assert os.path.isfile(checkpoint_file_path)
    assert context.list_checkpoints() == ["my_checkpoint"]

    with pytest.raises(CheckpointError):
        context.get_checkpoint("my_checkpoint")


def test_get_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    obs = context.get_checkpoint("my_checkpoint")
    assert isinstance(obs, dict)
    assert {
        "validation_operator_name": "action_list_operator",
        "batches": [
            {
                "batch_kwargs": {
                    "path": "/Users/me/projects/my_project/data/data.csv",
                    "datasource": "my_filesystem_datasource",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": ["suite_one", "suite_two"],
            },
            {
                "batch_kwargs": {
                    "query": "SELECT * FROM users WHERE status = 1",
                    "datasource": "my_redshift_datasource",
                },
                "expectation_suite_names": ["suite_three"],
            },
        ],
    }


def test_get_checkpoint_default_validation_operator(empty_data_context):
    yaml = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {"batches": []}
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "foo.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    obs = context.get_checkpoint("foo")
    assert isinstance(obs, dict)
    expected = {
        "validation_operator_name": "action_list_operator",
        "batches": [],
    }
    assert expected == obs


def test_get_checkpoint_raises_error_on_missing_batches_key(empty_data_context):
    yaml = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
    }
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "foo.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(CheckpointError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_non_list_batches(empty_data_context):
    yaml = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": {"stuff": 33},
    }
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "foo.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(CheckpointError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_missing_expectation_suite_names(
    empty_data_context,
):
    yaml = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": [{"batch_kwargs": {"foo": 33},}],
    }
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "foo.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(CheckpointError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_missing_batch_kwargs(empty_data_context):
    yaml = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": [{"expectation_suite_names": ["foo"]}],
    }
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "foo.yml"
    )
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(CheckpointError) as e:
        context.get_checkpoint("foo")
