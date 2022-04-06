import json
import os
import shutil
from collections import OrderedDict

import pandas as pd
import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

import great_expectations as ge
import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationConfiguration, expectationSuiteSchema
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import (
    BaseDataContext,
    DataContext,
    ExplorerDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import PasswordMasker, file_relative_path
from great_expectations.dataset import Dataset
from great_expectations.datasource import (
    Datasource,
    LegacyDatasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.datasource.types.batch_kwargs import PathBatchKwargs
from great_expectations.util import (
    deep_filter_properties_iterable,
    gen_directory_tree_str,
    is_library_loadable,
)
from tests.test_utils import create_files_in_directory, safe_remove

try:
    from unittest import mock
except ImportError:
    from unittest import mock

yaml = YAML()

parameterized_expectation_suite_name = "my_dag_node.default"


@pytest.fixture()
def parameterized_expectation_suite():
    fixture_path = file_relative_path(
        __file__,
        "../test_fixtures/expectation_suites/parameterized_expectation_suite_fixture.json",
    )
    with open(
        fixture_path,
    ) as suite:
        return json.load(suite)


@pytest.fixture(scope="function")
def titanic_multibatch_data_context(
    tmp_path,
) -> DataContext:
    """
    Based on titanic_data_context, but with 2 identical batches of
    data asset "titanic"
    """
    project_path = tmp_path / "titanic_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(__file__, "../test_fixtures/great_expectations_titanic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )
    return ge.data_context.DataContext(context_path)


@pytest.fixture
def data_context_with_bad_datasource(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()

    This DataContext has a connection to a datasource named my_postgres_db
    which is not a valid datasource.

    It is used by test_get_batch_multiple_datasources_do_not_scan_all()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(fixture_dir, "great_expectations_bad_datasource.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    return ge.data_context.DataContext(context_path)


def test_create_duplicate_expectation_suite(titanic_data_context):
    # create new expectation suite
    assert titanic_data_context.create_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite"
    )
    # attempt to create expectation suite with name that already exists on data asset
    with pytest.raises(ge_exceptions.DataContextError):
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
        batch_kwargs_generators={
            "foo": {
                "class_name": "TableBatchKwargsGenerator",
            }
        },
        **connection_kwargs,
    )
    context.add_datasource(
        "second", class_name="SqlAlchemyDatasource", **connection_kwargs
    )
    context.add_datasource(
        "third",
        class_name="SqlAlchemyDatasource",
        batch_kwargs_generators={
            "bar": {
                "class_name": "TableBatchKwargsGenerator",
            }
        },
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
        ExpectationSuiteIdentifier(
            expectation_suite_name=parameterized_expectation_suite_name
        )
    ]


def test_get_existing_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            parameterized_expectation_suite_name
        )
    )
    assert (
        expectation_suite.expectation_suite_name == parameterized_expectation_suite_name
    )
    assert len(expectation_suite.expectations) == 2


def test_get_new_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = (
        data_context_parameterized_expectation_suite.create_expectation_suite(
            "this_data_asset_does_not_exist.default"
        )
    )
    assert (
        expectation_suite.expectation_suite_name
        == "this_data_asset_does_not_exist.default"
    )
    assert len(expectation_suite.expectations) == 0


def test_save_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = (
        data_context_parameterized_expectation_suite.create_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    expectation_suite.expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite
    )
    expectation_suite_saved = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    assert expectation_suite.expectations == expectation_suite_saved.expectations


def test_compile_evaluation_parameter_dependencies(
    data_context_parameterized_expectation_suite: DataContext,
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

    if is_library_loadable(library_name="psycopg2"):

        # Make sure passwords are masked in password or url fields
        data_context_parameterized_expectation_suite.add_datasource(
            "postgres_source_with_password",
            initialize=False,
            module_name="great_expectations.datasource",
            class_name="SqlAlchemyDatasource",
            credentials={
                "drivername": "postgresql",
                "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                "port": "65432",
                "username": "username_str",
                "password": "password_str",
                "database": "database_str",
            },
        )

        data_context_parameterized_expectation_suite.add_datasource(
            "postgres_source_with_password_in_url",
            initialize=False,
            module_name="great_expectations.datasource",
            class_name="SqlAlchemyDatasource",
            credentials={
                "url": "postgresql+psycopg2://username:password@host:65432/database",
            },
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
            {
                "name": "postgres_source_with_password",
                "class_name": "SqlAlchemyDatasource",
                "module_name": "great_expectations.datasource",
                "data_asset_type": {
                    "class_name": "SqlAlchemyDataset",
                    "module_name": "great_expectations.dataset",
                },
                "credentials": {
                    "drivername": "postgresql",
                    "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                    "port": "65432",
                    "username": "username_str",
                    "password": PasswordMasker.MASKED_PASSWORD_STRING,
                    "database": "database_str",
                },
            },
            {
                "name": "postgres_source_with_password_in_url",
                "class_name": "SqlAlchemyDatasource",
                "module_name": "great_expectations.datasource",
                "data_asset_type": {
                    "class_name": "SqlAlchemyDataset",
                    "module_name": "great_expectations.dataset",
                },
                "credentials": {
                    "url": f"postgresql+psycopg2://username:{PasswordMasker.MASKED_PASSWORD_STRING}@host:65432/database",
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


def test_data_context_get_latest_validation_result(titanic_data_context):
    """
    Test that the latest validation result can be correctly fetched from the configured results
    store
    """
    for _ in range(2):
        titanic_data_context.profile_datasource("mydatasource")
    assert len(titanic_data_context.validations_store.list_keys()) == 2

    validation_results = [
        titanic_data_context.validations_store.get(val_key)
        for val_key in titanic_data_context.validations_store.list_keys()
    ]
    latest_validation_result = titanic_data_context.get_validation_result(
        "mydatasource.mygenerator.Titanic.BasicDatasetProfiler"
    )
    assert latest_validation_result in validation_results


def test_data_context_get_datasource(titanic_data_context):
    isinstance(titanic_data_context.get_datasource("mydatasource"), LegacyDatasource)


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
def test_render_full_static_site_from_empty_project(tmp_path, filesystem_csv_3):

    # TODO : Use a standard test fixture
    # TODO : Have that test fixture copy a directory, rather than building a new one from scratch

    project_dir = os.path.join(tmp_path, "project_path")
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
            .ge_store_backend_id
            titanic/
                subdir_reader/
                    Titanic/
                        BasicDatasetProfiler.json
        plugins/
            custom_data_docs/
                renderers/
                styles/
                    data_docs_custom_styles.css
                views/
        profilers/
        uncommitted/
            config_variables.yml
            data_docs/
            validations/
                .ge_store_backend_id
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

    # save data_docs locally if you need to inspect the files manually
    # os.makedirs("./tests/data_context/output", exist_ok=True)
    # os.makedirs("./tests/data_context/output/data_docs", exist_ok=True)
    #
    # if os.path.isdir("./tests/data_context/output/data_docs"):
    #     shutil.rmtree("./tests/data_context/output/data_docs")
    # shutil.copytree(
    #     os.path.join(ge_directory, "uncommitted/data_docs/"),
    #     "./tests/data_context/output/data_docs",
    # )


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


# noinspection PyPep8Naming
def test_ExplorerDataContext(titanic_data_context):
    context_root_directory = titanic_data_context.root_directory
    explorer_data_context = ExplorerDataContext(context_root_directory)
    assert explorer_data_context._expectation_explorer_manager


# noinspection PyPep8Naming
def test_ConfigOnlyDataContext__initialization(
    tmp_path_factory, basic_data_context_config
):
    config_path = str(
        tmp_path_factory.mktemp("test_ConfigOnlyDataContext__initialization__dir")
    )
    context = BaseDataContext(
        basic_data_context_config,
        config_path,
    )

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
    context = BaseDataContext(
        basic_data_context_config,
        config_path,
    )

    assert str(
        os.path.join("test__normalize_absolute_or_relative_path__dir0", "yikes")
    ) in context._normalize_absolute_or_relative_path("yikes")

    assert (
        "test__normalize_absolute_or_relative_path__dir"
        not in context._normalize_absolute_or_relative_path("/yikes")
    )
    assert "/yikes" == context._normalize_absolute_or_relative_path("/yikes")


def test_load_data_context_from_environment_variables(tmp_path, monkeypatch):
    project_path = tmp_path / "data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    assert os.path.isdir(context_path)
    monkeypatch.chdir(context_path)
    with pytest.raises(ge_exceptions.DataContextError) as err:
        DataContext.find_context_root_dir()
    assert isinstance(err.value, ge_exceptions.ConfigNotFoundError)

    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join("..", "test_fixtures", "great_expectations_basic.yml"),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    monkeypatch.setenv("GE_HOME", context_path)
    assert DataContext.find_context_root_dir() == context_path


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
    expectation_suite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            expectation_suite_name
        )
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

    fetched_expectation_suite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "a_new_suite_name"
        )
    )

    assert fetched_expectation_suite.expectation_suite_name == "a_new_suite_name"

    #   2. Using a different name that should be overwritten
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite,
        expectation_suite_name="a_new_new_suite_name",
    )

    fetched_expectation_suite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "a_new_new_suite_name"
        )
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
        loaded_suite_dict: dict = expectationSuiteSchema.load(json.load(suite_file))
        loaded_suite: ExpectationSuite = ExpectationSuite(
            **loaded_suite_dict,
            data_context=data_context_parameterized_expectation_suite,
        )
        assert loaded_suite.expectation_suite_name == "a_new_new_suite_name"

    #   3. Using the new name but having the context draw that from the suite
    expectation_suite.expectation_suite_name = "a_third_suite_name"
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite
    )

    fetched_expectation_suite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "a_third_suite_name"
        )
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
        .ge_store_backend_id
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    profilers/
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
            .ge_store_backend_id
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
        .ge_store_backend_id
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    profilers/
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
            .ge_store_backend_id
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
        .ge_store_backend_id
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
        "plugins",
        "profilers",
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


def test_scaffold_directories(tmp_path_factory):
    empty_directory = str(tmp_path_factory.mktemp("test_scaffold_directories"))
    DataContext.scaffold_directories(empty_directory)

    assert set(os.listdir(empty_directory)) == {
        "plugins",
        "checkpoints",
        "profilers",
        "expectations",
        ".gitignore",
        "uncommitted",
    }
    assert set(os.listdir(os.path.join(empty_directory, "uncommitted"))) == {
        "data_docs",
        "validations",
    }


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


def test_load_config_variables_file(
    basic_data_context_config, tmp_path_factory, monkeypatch
):
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
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "dev")
        context = BaseDataContext(basic_data_context_config, context_root_dir=base_path)
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "dev"
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "prod")
        context = BaseDataContext(basic_data_context_config, context_root_dir=base_path)
        config_vars = context._load_config_variables_file()
        assert config_vars["env"] == "prod"
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        monkeypatch.delenv("TEST_CONFIG_FILE_ENV")


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
    with pytest.raises(ge_exceptions.DataContextError):
        titanic_data_context.get_batch({}, 99)


def test_get_batch_raises_error_when_passed_a_non_dict_or_batch_kwarg_type_for_batch_kwarg_parameter(
    titanic_data_context,
):
    with pytest.raises(ge_exceptions.BatchKwargsError):
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
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_checkpoint.yml",
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
            context.root_directory,
            DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
            f"foo{extension}",
        )
        with open(path, "w") as f:
            f.write("foo: bar")
        assert os.path.isfile(path)

    assert context.list_checkpoints() == ["my_checkpoint"]


def test_get_checkpoint_raises_error_on_not_found_checkpoint(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    with pytest.raises(ge_exceptions.CheckpointNotFoundError):
        context.get_checkpoint("not_a_checkpoint")


def test_get_checkpoint_raises_error_empty_checkpoint(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    checkpoint_file_path = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_checkpoint.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        f.write("# Not a Checkpoint file")
    assert os.path.isfile(checkpoint_file_path)
    assert context.list_checkpoints() == ["my_checkpoint"]

    with pytest.raises(ge_exceptions.InvalidCheckpointConfigError):
        context.get_checkpoint("my_checkpoint")


def test_get_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    obs = context.get_checkpoint("my_checkpoint")
    assert isinstance(obs, Checkpoint)
    config = obs.get_config(mode=ConfigOutputModes.JSON_DICT)
    assert isinstance(config, dict)
    assert config == {
        "name": "my_checkpoint",
        "class_name": "LegacyCheckpoint",
        "module_name": "great_expectations.checkpoint",
        "batches": [
            {
                "batch_kwargs": {
                    "datasource": "my_filesystem_datasource",
                    "path": "/Users/me/projects/my_project/data/data.csv",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": ["suite_one", "suite_two"],
            },
            {
                "batch_kwargs": {
                    "datasource": "my_redshift_datasource",
                    "query": "SELECT * FROM users WHERE status = 1",
                },
                "expectation_suite_names": ["suite_three"],
            },
        ],
        "validation_operator_name": "action_list_operator",
    }


def test_get_checkpoint_raises_error_on_missing_batches_key(empty_data_context):
    yaml_obj = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
    }
    checkpoint_file_path = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "foo.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        yaml_obj.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(ge_exceptions.CheckpointError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_non_list_batches(empty_data_context):
    yaml_obj = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": {"stuff": 33},
    }
    checkpoint_file_path = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "foo.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        yaml_obj.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(ge_exceptions.InvalidCheckpointConfigError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_missing_expectation_suite_names(
    empty_data_context,
):
    yaml_obj = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": [
            {
                "batch_kwargs": {"foo": 33},
            }
        ],
    }
    checkpoint_file_path = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "foo.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        yaml_obj.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(ge_exceptions.CheckpointError) as e:
        context.get_checkpoint("foo")


def test_get_checkpoint_raises_error_on_missing_batch_kwargs(empty_data_context):
    yaml_obj = YAML(typ="safe")
    context = empty_data_context

    checkpoint = {
        "validation_operator_name": "action_list_operator",
        "batches": [{"expectation_suite_names": ["foo"]}],
    }
    checkpoint_file_path = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "foo.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        yaml_obj.dump(checkpoint, f)
    assert os.path.isfile(checkpoint_file_path)

    with pytest.raises(ge_exceptions.CheckpointError) as e:
        context.get_checkpoint("foo")


# TODO: add more test cases
def test_run_checkpoint_new_style(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # add Checkpoint config
    checkpoint_config = CheckpointConfig(
        name="my_checkpoint",
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_basic_data_connector",
                    "data_asset_name": "Titanic_1911",
                }
            }
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(
        configuration_key=checkpoint_config.name
    )
    context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)

    with pytest.raises(
        ge_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        context.run_checkpoint(checkpoint_name=checkpoint_config.name)

    assert len(context.validations_store.list_keys()) == 0

    # print(context.list_datasources())

    context.create_expectation_suite(expectation_suite_name="my_expectation_suite")

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name=checkpoint_config.name
    )
    assert len(result.list_validation_results()) == 1
    assert result.success

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name=checkpoint_config.name
    )
    assert len(result.list_validation_results()) == 1
    assert len(context.validations_store.list_keys()) == 2
    assert result.success


def test_get_validator_with_instantiated_expectation_suite(
    empty_data_context_stats_enabled, tmp_path_factory
):
    context: DataContext = empty_data_context_stats_enabled

    base_directory = str(
        tmp_path_factory.mktemp(
            "test_get_validator_with_instantiated_expectation_suite"
        )
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_file.csv",
        ],
    )

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}
        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric
        assets:
            A:
"""

    config = yaml.load(yaml_config)
    context.add_datasource(
        "my_directory_datasource",
        **config,
    )

    my_validator = context.get_validator(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_identifiers={
            "alphanumeric": "some_file",
        },
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    assert my_validator.expectation_suite_name == "my_expectation_suite"


def test_get_validator_with_attach_expectation_suite(
    empty_data_context, tmp_path_factory
):
    context = empty_data_context

    base_directory = str(
        tmp_path_factory.mktemp("test_get_validator_with_attach_expectation_suite")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_file.csv",
        ],
    )

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}
        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric
        assets:
            A:
"""

    config = yaml.load(yaml_config)
    context.add_datasource(
        "my_directory_datasource",
        **config,
    )

    my_validator = context.get_validator(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_identifiers={
            "alphanumeric": "some_file",
        },
        create_expectation_suite_with_name="A_expectation_suite",
    )
    assert my_validator.expectation_suite_name == "A_expectation_suite"

def test_get_validator_without_expectation_suite(
    empty_data_context_stats_enabled, tmp_path_factory
):
    context: DataContext = empty_data_context_stats_enabled

    base_directory = str(
        tmp_path_factory.mktemp(
            "test_get_validator_without_expectation_suite"
        )
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_file.csv",
        ],
    )

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}
        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric
        assets:
            A:
"""

    config = yaml.load(yaml_config)
    context.add_datasource(
        "my_directory_datasource",
        **config,
    )

    my_validator = context.get_validator(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_identifiers={
            "alphanumeric": "some_file",
        },
    )
    assert type(my_validator.get_expectation_suite()) == ExpectationSuite
    assert my_validator.expectation_suite_name == "default"

def test_get_validator_with_batch(
    empty_data_context, tmp_path_factory
):
    context = empty_data_context

    base_directory = str(
        tmp_path_factory.mktemp("test_get_validator_with_batch")
    )

    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "some_file.csv",
        ],
    )

    yaml_config = f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}
        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - alphanumeric
        assets:
            A:
"""

    config = yaml.load(yaml_config)
    context.add_datasource(
        "my_directory_datasource",
        **config,
    )

    my_batch = context.get_batch_list(
        datasource_name="my_directory_datasource",
        data_connector_name="my_filesystem_data_connector",
        data_asset_name="A",
        batch_identifiers={
            "alphanumeric": "some_file",
        },
    )[0]

    my_validator = context.get_validator(
        batch=my_batch,
        create_expectation_suite_with_name="A_expectation_suite",
    )

def test_get_validator_with_batch_list(
    in_memory_runtime_context
):
    context = in_memory_runtime_context

    my_batch_list = [
        context.get_batch(
            batch_request=RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "batch_data": pd.DataFrame({
                        "x": range(10)
                    })
                },
                batch_identifiers={
                    "id_key_0": "id_0_value_a",
                    "id_key_1": "id_1_value_a",
                },
            )
        ),
        context.get_batch(
            batch_request=RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "batch_data": pd.DataFrame({
                        "y": range(10)
                    })
                },
                batch_identifiers={
                    "id_key_0": "id_0_value_b",
                    "id_key_1": "id_1_value_b",
                },
            )
        ),
    ]

    my_validator = context.get_validator(
        batch_list=my_batch_list,
        create_expectation_suite_with_name="A_expectation_suite",
    )
    assert len(my_validator.batches) == 2


def test_get_batch_multiple_datasources_do_not_scan_all(
    data_context_with_bad_datasource,
):
    """
    What does this test and why?

    A DataContext can have "stale" datasources in its configuration (ie. connections to DBs that are now offline).
    If we configure a new datasource and are only using it (like the PandasDatasource below), then we don't
    want to be dependent on all the "stale" datasources working too.

    data_context_with_bad_datasource is a fixture that contains a configuration for an invalid datasource
    (with "fake_port" and "fake_host")

    In the test we configure a new expectation_suite, a local pandas_datasource and retrieve a single batch.

    This tests a fix for the following issue:
    https://github.com/great-expectations/great_expectations/issues/2241
    """

    context = data_context_with_bad_datasource
    context.create_expectation_suite(expectation_suite_name="local_test.default")
    expectation_suite = context.get_expectation_suite("local_test.default")
    context.add_datasource("pandas_datasource", class_name="PandasDatasource")
    df = pd.DataFrame({"a": [1, 2, 3]})
    batch = context.get_batch(
        batch_kwargs={"datasource": "pandas_datasource", "dataset": df},
        expectation_suite_name=expectation_suite,
    )
    assert len(batch) == 3


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_expectation_to_expectation_suite(
    mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    expectation_suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="my_new_expectation_suite"
    )
    expectation_suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "expectation_suite.add_expectation",
                "event_payload": {},
                "success": True,
            }
        )
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_checkpoint_from_yaml(mock_emit, empty_data_context_stats_enabled):
    """
    What does this test and why?
    We should be able to add a checkpoint directly from a valid yaml configuration.
    test_yaml_config() should not automatically save a checkpoint if valid.
    checkpoint yaml in a store should match the configuration, even if created from SimpleCheckpoints
    Note: This tests multiple items and could stand to be broken up.
    """

    context: DataContext = empty_data_context_stats_enabled
    checkpoint_name: str = "my_new_checkpoint"

    assert checkpoint_name not in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 0

    checkpoint_yaml_config = f"""
name: {checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: data_dir
      data_connector_name: data_dir_example_data_connector
      data_asset_name: DEFAULT_ASSET_NAME
      partition_request:
        index: -1
    expectation_suite_name: newsuite
    """

    checkpoint_from_test_yaml_config = context.test_yaml_config(
        checkpoint_yaml_config, name=checkpoint_name
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "SimpleCheckpoint",
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list

    # test_yaml_config() no longer stores checkpoints automatically
    assert checkpoint_name not in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 0

    checkpoint_from_yaml = context.add_checkpoint(
        **yaml.load(checkpoint_yaml_config),
    )

    expected_checkpoint_yaml: str = """name: my_new_checkpoint
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request: {}
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
evaluation_parameters: {}
runtime_configuration: {}
validations:
  - batch_request:
      datasource_name: data_dir
      data_connector_name: data_dir_example_data_connector
      data_asset_name: DEFAULT_ASSET_NAME
      partition_request:
        index: -1
    expectation_suite_name: newsuite
profilers: []
ge_cloud_id:
expectation_suite_ge_cloud_id:
"""

    checkpoint_dir = os.path.join(
        context.root_directory,
        context.checkpoint_store.config["store_backend"]["base_directory"],
    )
    checkpoint_file = os.path.join(checkpoint_dir, f"{checkpoint_name}.yml")

    with open(checkpoint_file) as cf:
        checkpoint_from_disk = cf.read()

    assert checkpoint_from_disk == expected_checkpoint_yaml
    assert (
        checkpoint_from_yaml.get_config(mode=ConfigOutputModes.YAML)
        == expected_checkpoint_yaml
    )
    assert deep_filter_properties_iterable(
        properties=checkpoint_from_yaml.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=dict(yaml.load(expected_checkpoint_yaml)),
        clean_falsy=True,
    )

    checkpoint_from_store = context.get_checkpoint(name=checkpoint_name)
    assert (
        checkpoint_from_store.get_config(mode=ConfigOutputModes.YAML)
        == expected_checkpoint_yaml
    )
    assert deep_filter_properties_iterable(
        properties=checkpoint_from_store.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=dict(yaml.load(expected_checkpoint_yaml)),
        clean_falsy=True,
    )

    expected_action_list = [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
        },
    ]

    assert checkpoint_from_yaml.action_list == expected_action_list
    assert checkpoint_from_store.action_list == expected_action_list
    assert checkpoint_from_test_yaml_config.action_list == expected_action_list
    assert checkpoint_from_store.action_list == expected_action_list

    assert checkpoint_from_test_yaml_config.name == checkpoint_from_yaml.name
    assert (
        checkpoint_from_test_yaml_config.action_list == checkpoint_from_yaml.action_list
    )

    assert checkpoint_from_yaml.name == checkpoint_name
    assert checkpoint_from_yaml.get_config(
        mode=ConfigOutputModes.JSON_DICT, clean_falsy=True
    ) == {
        "name": "my_new_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "data_dir",
                    "data_connector_name": "data_dir_example_data_connector",
                    "data_asset_name": "DEFAULT_ASSET_NAME",
                    "partition_request": {"index": -1},
                },
                "expectation_suite_name": "newsuite",
            }
        ],
    }

    assert isinstance(checkpoint_from_yaml, Checkpoint)

    assert checkpoint_name in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 1

    # No other usage stats calls detected
    assert mock_emit.call_count == 1


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_checkpoint_from_yaml_fails_for_unrecognized_class_name(
    mock_emit, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    Checkpoint yaml should have a valid class_name
    """

    context: DataContext = empty_data_context_stats_enabled
    checkpoint_name: str = "my_new_checkpoint"

    assert checkpoint_name not in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 0

    checkpoint_yaml_config = f"""
name: {checkpoint_name}
config_version: 1.0
class_name: NotAValidCheckpointClassName
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: data_dir
      data_connector_name: data_dir_example_data_connector
      data_asset_name: DEFAULT_ASSET_NAME
      partition_request:
        index: -1
    expectation_suite_name: newsuite
    """

    with pytest.raises(KeyError):
        context.test_yaml_config(checkpoint_yaml_config, name=checkpoint_name)

    with pytest.raises(AttributeError):
        context.add_checkpoint(
            **yaml.load(checkpoint_yaml_config),
        )

    assert checkpoint_name not in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 0
    assert mock_emit.call_count == 1
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {},
                "success": False,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_datasource_from_yaml(mock_emit, empty_data_context_stats_enabled):
    """
    What does this test and why?
    Adding a datasource using context.add_datasource() via a config from a parsed yaml string without substitution variables should work as expected.
    """
    context: DataContext = empty_data_context_stats_enabled

    assert "my_new_datasource" not in context.datasources.keys()
    assert "my_new_datasource" not in context.list_datasources()
    assert "my_new_datasource" not in context.get_config()["datasources"]

    datasource_name: str = "my_datasource"

    example_yaml = f"""
    class_name: Datasource
    execution_engine:
      class_name: PandasExecutionEngine
    data_connectors:
      data_dir_example_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        datasource_name: {datasource_name}
        base_directory: ../data
        default_regex:
          group_names: data_asset_name
          pattern: (.*)
    """
    datasource_from_test_yaml_config = context.test_yaml_config(
        example_yaml, name=datasource_name
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized names since it changes for each run
    anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
        "event_payload"
    ]["anonymized_execution_engine"]["anonymized_name"]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": anonymized_execution_engine_name,
                        "parent_class": "PandasExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetFilesystemDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list

    datasource_from_yaml = context.add_datasource(
        name=datasource_name, **yaml.load(example_yaml)
    )
    assert mock_emit.call_count == 2
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.add_datasource",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list

    assert datasource_from_test_yaml_config.config == datasource_from_yaml.config

    assert datasource_from_yaml.name == datasource_name
    assert datasource_from_yaml.config == {
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "data_dir_example_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "default_regex": {"group_names": "data_asset_name", "pattern": "(.*)"},
                "base_directory": "../data",
            }
        },
    }
    assert isinstance(datasource_from_yaml, Datasource)
    assert datasource_from_yaml.__class__.__name__ == "Datasource"

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]

    # Check that the datasource was written to disk as expected
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]
    assert mock_emit.call_count == 3
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.__init__",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_datasource_from_yaml_sql_datasource(
    mock_emit,
    sa,
    test_backends,
    empty_data_context_stats_enabled,
):
    """
    What does this test and why?
    Adding a datasource using context.add_datasource() via a config from a parsed yaml string without substitution variables should work as expected.
    """

    if "postgresql" not in test_backends:
        pytest.skip("test_add_datasource_from_yaml_sql_datasource requires postgresql")

    context: DataContext = empty_data_context_stats_enabled

    assert "my_new_datasource" not in context.datasources.keys()
    assert "my_new_datasource" not in context.list_datasources()
    assert "my_new_datasource" not in context.get_config()["datasources"]

    datasource_name: str = "my_datasource"

    example_yaml = """
    class_name: SimpleSqlalchemyDatasource
    introspection:
      whole_table:
        data_asset_name_suffix: __whole_table
    credentials:
      drivername: postgresql
      host: localhost
      port: '5432'
      username: postgres
      password: ''
      database: postgres
    """

    datasource_from_test_yaml_config = context.test_yaml_config(
        example_yaml, name=datasource_name
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "SimpleSqlalchemyDatasource",
                    "anonymized_execution_engine": {
                        "parent_class": "SqlAlchemyExecutionEngine"
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetSqlDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list
    datasource_from_yaml = context.add_datasource(
        name=datasource_name, **yaml.load(example_yaml)
    )
    assert mock_emit.call_count == 2
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.add_datasource",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list

    # .config not implemented for SimpleSqlalchemyDatasource
    assert datasource_from_test_yaml_config.config == {}
    assert datasource_from_yaml.config == {}

    assert datasource_from_yaml.name == datasource_name
    assert isinstance(datasource_from_yaml, SimpleSqlalchemyDatasource)
    assert datasource_from_yaml.__class__.__name__ == "SimpleSqlalchemyDatasource"

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]

    assert isinstance(
        context.get_datasource(datasource_name=datasource_name),
        SimpleSqlalchemyDatasource,
    )
    assert isinstance(
        context.get_config()["datasources"][datasource_name], DatasourceConfig
    )

    # As of 20210312 SimpleSqlalchemyDatasource returns an empty {} .config
    # so here we check for each part of the config individually
    datasource_config = context.get_config()["datasources"][datasource_name]
    assert datasource_config.class_name == "SimpleSqlalchemyDatasource"
    assert datasource_config.credentials == {
        "drivername": "postgresql",
        "host": "localhost",
        "port": "5432",
        "username": "postgres",
        "password": "",
        "database": "postgres",
    }
    assert datasource_config.credentials == OrderedDict(
        [
            ("drivername", "postgresql"),
            ("host", "localhost"),
            ("port", "5432"),
            ("username", "postgres"),
            ("password", ""),
            ("database", "postgres"),
        ]
    )
    assert datasource_config.introspection == OrderedDict(
        [("whole_table", OrderedDict([("data_asset_name_suffix", "__whole_table")]))]
    )
    assert datasource_config.module_name == "great_expectations.datasource"

    # Check that the datasource was written to disk as expected
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]

    assert isinstance(
        context.get_datasource(datasource_name=datasource_name),
        SimpleSqlalchemyDatasource,
    )
    assert isinstance(
        context.get_config()["datasources"][datasource_name], DatasourceConfig
    )

    # As of 20210312 SimpleSqlalchemyDatasource returns an empty {} .config
    # so here we check for each part of the config individually
    datasource_config = context.get_config()["datasources"][datasource_name]
    assert datasource_config.class_name == "SimpleSqlalchemyDatasource"
    assert datasource_config.credentials == {
        "drivername": "postgresql",
        "host": "localhost",
        "port": "5432",
        "username": "postgres",
        "password": "",
        "database": "postgres",
    }
    assert datasource_config.credentials == OrderedDict(
        [
            ("drivername", "postgresql"),
            ("host", "localhost"),
            ("port", "5432"),
            ("username", "postgres"),
            ("password", ""),
            ("database", "postgres"),
        ]
    )
    assert datasource_config.introspection == OrderedDict(
        [("whole_table", OrderedDict([("data_asset_name_suffix", "__whole_table")]))]
    )
    assert datasource_config.module_name == "great_expectations.datasource"
    assert mock_emit.call_count == 3
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.__init__",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_datasource_from_yaml_sql_datasource_with_credentials(
    mock_emit, sa, test_backends, empty_data_context_stats_enabled
):
    """
    What does this test and why?
    Adding a datasource using context.add_datasource() via a config from a parsed yaml string without substitution variables.
    In addition, this tests whether the same can be accomplished using credentials with a  Datasource and SqlAlchemyExecutionEngine, rather than a SimpleSqlalchemyDatasource
    """

    if "postgresql" not in test_backends:
        pytest.skip(
            "test_add_datasource_from_yaml_sql_datasource_with_credentials requires postgresql"
        )

    context: DataContext = empty_data_context_stats_enabled

    assert "my_new_datasource" not in context.datasources.keys()
    assert "my_new_datasource" not in context.list_datasources()
    assert "my_new_datasource" not in context.get_config()["datasources"]

    datasource_name: str = "my_datasource"

    example_yaml = """
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      credentials:
        host: localhost
        port: 5432
        username: postgres
        password:
        database: test_ci
        drivername: postgresql
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        name: whole_table
      default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name
    """

    datasource_from_test_yaml_config = context.test_yaml_config(
        example_yaml, name=datasource_name
    )
    assert mock_emit.call_count == 1
    # Substitute anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
        "event_payload"
    ]["anonymized_execution_engine"]["anonymized_name"]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    anonymized_data_connector_name_1 = mock_emit.call_args_list[0][0][0][
        "event_payload"
    ]["anonymized_data_connectors"][1]["anonymized_name"]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": anonymized_execution_engine_name,
                        "parent_class": "SqlAlchemyExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetSqlDataConnector",
                        },
                        {
                            "anonymized_name": anonymized_data_connector_name_1,
                            "parent_class": "RuntimeDataConnector",
                        },
                    ],
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list
    datasource_from_yaml = context.add_datasource(
        name=datasource_name, **yaml.load(example_yaml)
    )
    assert mock_emit.call_count == 2
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.add_datasource",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list

    assert datasource_from_test_yaml_config.config == {
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "credentials": {
                "host": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": None,
                "database": "test_ci",
                "drivername": "postgresql",
            },
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
                "module_name": "great_expectations.datasource.data_connector",
            },
        },
    }
    assert datasource_from_yaml.config == {
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "credentials": {
                "host": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": None,
                "database": "test_ci",
                "drivername": "postgresql",
            },
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
                "module_name": "great_expectations.datasource.data_connector",
            },
        },
    }

    assert datasource_from_yaml.name == datasource_name
    assert isinstance(datasource_from_yaml, Datasource)
    assert datasource_from_yaml.__class__.__name__ == "Datasource"

    assert datasource_name == context.list_datasources()[0]["name"]
    assert isinstance(context.datasources[datasource_name], Datasource)

    assert isinstance(
        context.get_datasource(datasource_name=datasource_name),
        Datasource,
    )
    assert isinstance(
        context.get_config()["datasources"][datasource_name], DatasourceConfig
    )

    # making sure the config is right
    datasource_config = context.get_config()["datasources"][datasource_name]
    assert datasource_config.class_name == "Datasource"
    assert datasource_config.execution_engine.credentials == {
        "host": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": None,
        "database": "test_ci",
        "drivername": "postgresql",
    }
    assert datasource_config.execution_engine.credentials == OrderedDict(
        [
            ("host", "localhost"),
            ("port", 5432),
            ("username", "postgres"),
            ("password", None),
            ("database", "test_ci"),
            ("drivername", "postgresql"),
        ]
    )

    # No other usage stats calls detected
    assert mock_emit.call_count == 2


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_datasource_from_yaml_with_substitution_variables(
    mock_emit, empty_data_context_stats_enabled, monkeypatch
):
    """
    What does this test and why?
    Adding a datasource using context.add_datasource() via a config from a parsed yaml string containing substitution variables should work as expected.
    """

    context: DataContext = empty_data_context_stats_enabled

    assert "my_new_datasource" not in context.datasources.keys()
    assert "my_new_datasource" not in context.list_datasources()
    assert "my_new_datasource" not in context.get_config()["datasources"]

    datasource_name: str = "my_datasource"

    monkeypatch.setenv("SUBSTITUTED_BASE_DIRECTORY", "../data")

    example_yaml = f"""
        class_name: Datasource
        execution_engine:
          class_name: PandasExecutionEngine
        data_connectors:
          data_dir_example_data_connector:
            class_name: InferredAssetFilesystemDataConnector
            datasource_name: {datasource_name}
            base_directory: ${{SUBSTITUTED_BASE_DIRECTORY}}
            default_regex:
              group_names: data_asset_name
              pattern: (.*)
        """
    datasource_from_test_yaml_config = context.test_yaml_config(
        example_yaml, name=datasource_name
    )

    assert mock_emit.call_count == 1
    # Substitute anonymized names since it changes for each run
    anonymized_datasource_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    anonymized_execution_engine_name = mock_emit.call_args_list[0][0][0][
        "event_payload"
    ]["anonymized_execution_engine"]["anonymized_name"]
    anonymized_data_connector_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_data_connectors"
    ][0]["anonymized_name"]
    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_datasource_name,
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": anonymized_execution_engine_name,
                        "parent_class": "PandasExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": anonymized_data_connector_name,
                            "parent_class": "InferredAssetFilesystemDataConnector",
                        }
                    ],
                },
                "success": True,
            }
        ),
    ]
    assert mock_emit.call_args_list == expected_call_args_list
    datasource_from_yaml = context.add_datasource(
        name=datasource_name, **yaml.load(example_yaml)
    )
    assert mock_emit.call_count == 2
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.add_datasource",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list

    assert datasource_from_test_yaml_config.config == datasource_from_yaml.config

    assert datasource_from_yaml.name == datasource_name
    assert datasource_from_yaml.config == {
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        "data_connectors": {
            "data_dir_example_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "default_regex": {"group_names": "data_asset_name", "pattern": "(.*)"},
                "base_directory": "../data",
            }
        },
    }
    assert isinstance(datasource_from_yaml, Datasource)
    assert datasource_from_yaml.__class__.__name__ == "Datasource"

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]

    # Check that the datasource was written to disk as expected
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)

    assert datasource_name in [d["name"] for d in context.list_datasources()]
    assert datasource_name in context.datasources
    assert datasource_name in context.get_config()["datasources"]
    assert mock_emit.call_count == 3
    expected_call_args_list.extend(
        [
            mock.call(
                {
                    "event": "data_context.__init__",
                    "event_payload": {},
                    "success": True,
                }
            ),
        ]
    )
    assert mock_emit.call_args_list == expected_call_args_list


def test_stores_evaluation_parameters_resolve_correctly(data_context_with_query_store):
    """End to end test demonstrating usage of Stores evaluation parameters"""
    context = data_context_with_query_store
    suite_name = "eval_param_suite"
    context.create_expectation_suite(expectation_suite_name=suite_name)
    batch_request = {
        "datasource_name": "my_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "DEFAULT_ASSET_NAME",
        "batch_identifiers": {"default_identifier_name": "test123"},
        "runtime_parameters": {"query": "select * from titanic"},
    }
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite_name=suite_name,
    )
    validator.expect_table_row_count_to_equal(
        value={
            # unnecessarily complex URN which should resolve to the actual row count.
            "$PARAMETER": "abs(-urn:great_expectations:stores:my_query_store:col_count - urn:great_expectations:stores:my_query_store:dist_col_count) + 4"
        }
    )

    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {"batch_request": batch_request, "expectation_suite_name": suite_name}
        ],
    }
    checkpoint = SimpleCheckpoint(
        f"_tmp_checkpoint_{suite_name}", context, **checkpoint_config
    )
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.get("success") is True
