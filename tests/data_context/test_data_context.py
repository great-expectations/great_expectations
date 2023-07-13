import copy
import json
import os
import shutil
from collections import OrderedDict
from typing import Dict, List, Union

import pandas as pd
import pytest
from freezegun import freeze_time

import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationConfiguration, expectationSuiteSchema
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import DataContext
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset import Dataset
from great_expectations.datasource import (
    Datasource,
    LegacyDatasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.datasource.types.batch_kwargs import PathBatchKwargs
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.render import (
    AtomicPrescriptiveRendererType,
    AtomicRendererType,
    RenderedAtomicContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.util import (
    deep_filter_properties_iterable,
    gen_directory_tree_str,
    get_context,
)
from tests.test_utils import create_files_in_directory, safe_remove

try:
    from unittest import mock
except ImportError:
    from unittest import mock

yaml = YAMLHandler()

parameterized_expectation_suite_name = "my_dag_node.default"


@pytest.fixture(scope="function")
def titanic_multibatch_data_context(
    tmp_path,
) -> FileDataContext:
    """
    Based on titanic_data_context, but with 2 identical batches of
    data asset "titanic"
    """
    project_path = tmp_path / "titanic_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"), exist_ok=True  # noqa: PTH118
    )
    data_path = os.path.join(context_path, "..", "data", "titanic")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH103, PTH118
    shutil.copy(
        file_relative_path(__file__, "../test_fixtures/great_expectations_titanic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1911.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1912.csv"
            )
        ),
    )
    return get_context(context_root_dir=context_path)


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
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(  # noqa: PTH103
        os.path.join(asset_config_path, "my_dag_node"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_bad_datasource.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    return get_context(context_root_dir=context_path)


@pytest.mark.filesystem
def test_create_duplicate_expectation_suite(titanic_data_context):
    # create new expectation suite
    assert titanic_data_context.add_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite"
    )
    # attempt to create expectation suite with name that already exists on data asset
    with pytest.raises(gx_exceptions.DataContextError):
        titanic_data_context.add_expectation_suite(
            expectation_suite_name="titanic.test_create_expectation_suite"
        )
    # create expectation suite with name that already exists on data asset, but pass overwrite_existing=True
    assert titanic_data_context.add_or_update_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite",
    )


@pytest.mark.filesystem
def test_list_expectation_suite_keys(data_context_parameterized_expectation_suite):
    assert data_context_parameterized_expectation_suite.list_expectation_suites() == [
        ExpectationSuiteIdentifier(
            expectation_suite_name=parameterized_expectation_suite_name
        )
    ]


@pytest.mark.filesystem
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


@pytest.mark.filesystem
def test_get_new_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
            "this_data_asset_does_not_exist.default"
        )
    )
    assert (
        expectation_suite.expectation_suite_name
        == "this_data_asset_does_not_exist.default"
    )
    assert len(expectation_suite.expectations) == 0


@pytest.mark.filesystem
def test_save_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
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


@pytest.mark.filesystem
@pytest.mark.integration
def test_save_expectation_suite_include_rendered_content(
    data_context_parameterized_expectation_suite,
):
    expectation_suite: ExpectationSuite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    expectation_suite.expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    for expectation in expectation_suite.expectations:
        assert expectation.rendered_content is None
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite,
        include_rendered_content=True,
    )
    expectation_suite_saved: ExpectationSuite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    for expectation in expectation_suite_saved.expectations:
        for rendered_content_block in expectation.rendered_content:
            assert isinstance(
                rendered_content_block,
                RenderedAtomicContent,
            )


@pytest.mark.filesystem
@pytest.mark.integration
def test_get_expectation_suite_include_rendered_content(
    data_context_parameterized_expectation_suite,
):
    expectation_suite: ExpectationSuite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    expectation_suite.expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    for expectation in expectation_suite.expectations:
        assert expectation.rendered_content is None
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite,
    )
    (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    for expectation in expectation_suite.expectations:
        assert expectation.rendered_content is None

    expectation_suite_retrieved: ExpectationSuite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default",
            include_rendered_content=True,
        )
    )

    for expectation in expectation_suite_retrieved.expectations:
        for rendered_content_block in expectation.rendered_content:
            assert isinstance(
                rendered_content_block,
                RenderedAtomicContent,
            )


@pytest.mark.filesystem
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


@pytest.mark.filesystem
@mock.patch("great_expectations.data_context.store.DatasourceStore.update_by_name")
def test_update_datasource_persists_changes_with_store(
    mock_update_by_name: mock.MagicMock,
    data_context_parameterized_expectation_suite: DataContext,
) -> None:
    context: DataContext = data_context_parameterized_expectation_suite

    datasource_to_update: Datasource = tuple(context.datasources.values())[0]

    context.update_datasource(datasource=datasource_to_update)

    assert mock_update_by_name.call_count == 1


@pytest.mark.filesystem
@freeze_time("09/26/2019 13:42:41")
def test_data_context_get_validation_result(titanic_data_context):
    """
    Test that validation results can be correctly fetched from the configured results store
    """
    run_id = RunIdentifier(run_name="profiling")
    titanic_data_context.profile_datasource("mydatasource", run_id=run_id)

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


@pytest.mark.filesystem
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


@pytest.mark.unit
def test_data_context_get_datasource(titanic_data_context):
    isinstance(titanic_data_context.get_datasource("mydatasource"), LegacyDatasource)


@pytest.mark.filesystem
def test_data_context_expectation_suite_delete(empty_data_context):
    assert empty_data_context.add_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 1
    empty_data_context.delete_expectation_suite(
        expectation_suite_name=expectation_suites[0]
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 0


@pytest.mark.filesystem
def test_data_context_expectation_nested_suite_delete(empty_data_context):
    assert empty_data_context.add_expectation_suite(
        expectation_suite_name="titanic.test.create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert empty_data_context.add_expectation_suite(
        expectation_suite_name="titanic.test.a.create_expectation_suite"
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 2
    empty_data_context.delete_expectation_suite(
        expectation_suite_name=expectation_suites[0]
    )
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 1


@pytest.mark.unit
def test_data_context_get_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context,
):
    with pytest.raises(ValueError):
        _ = titanic_data_context.get_datasource("fakey_mc_fake")


@pytest.mark.unit
def test_data_context_profile_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context,
):
    with pytest.raises(ValueError):
        _ = titanic_data_context.profile_datasource("fakey_mc_fake")


@pytest.mark.filesystem
@freeze_time("09/26/2019 13:42:41")
@pytest.mark.rendered_output
@pytest.mark.slow  # 1.02s
def test_render_full_static_site_from_empty_project(tmp_path, filesystem_csv_3):
    # TODO : Use a standard test fixture
    # TODO : Have that test fixture copy a directory, rather than building a new one from scratch

    project_dir = os.path.join(tmp_path, "project_path")  # noqa: PTH118
    os.mkdir(project_dir)  # noqa: PTH102

    os.makedirs(os.path.join(project_dir, "data"))  # noqa: PTH103, PTH118
    os.makedirs(os.path.join(project_dir, "data/titanic"))  # noqa: PTH103, PTH118
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(project_dir, "data/titanic/Titanic.csv")),  # noqa: PTH118
    )

    os.makedirs(os.path.join(project_dir, "data/random"))  # noqa: PTH103, PTH118
    shutil.copy(
        os.path.join(filesystem_csv_3, "f1.csv"),  # noqa: PTH118
        str(os.path.join(project_dir, "data/random/f1.csv")),  # noqa: PTH118
    )
    shutil.copy(
        os.path.join(filesystem_csv_3, "f2.csv"),  # noqa: PTH118
        str(os.path.join(project_dir, "data/random/f2.csv")),  # noqa: PTH118
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

    context = FileDataContext.create(project_dir)
    context.add_datasource(
        "titanic",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": os.path.join(  # noqa: PTH118
                    project_dir, "data/titanic/"
                ),
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
                "base_directory": os.path.join(  # noqa: PTH118
                    project_dir, "data/random/"
                ),
            }
        },
    )

    context.profile_datasource("titanic")

    # Replicate the batch id of the batch that will be profiled in order to generate the file path of the
    # validation result
    titanic_profiled_batch_id = PathBatchKwargs(
        {
            "path": os.path.join(  # noqa: PTH118
                project_dir, "data/titanic/Titanic.csv"
            ),
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
            "path": os.path.join(project_dir, "data/random/f1.csv"),  # noqa: PTH118
            "datasource": "random",
            "data_asset_name": "f1",
        }
    ).to_id()

    f2_profiled_batch_id = PathBatchKwargs(
        {
            "path": os.path.join(project_dir, "data/random/f2.csv"),  # noqa: PTH118
            "datasource": "random",
            "data_asset_name": "f2",
        }
    ).to_id()

    data_docs_dir = os.path.join(  # noqa: PTH118
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


@pytest.mark.unit
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


@pytest.mark.unit
def test_ConfigOnlyDataContext__initialization(
    tmp_path_factory, basic_data_context_config
):
    config_path = str(
        tmp_path_factory.mktemp("test_ConfigOnlyDataContext__initialization__dir")
    )
    context = get_context(
        basic_data_context_config,
        config_path,
    )

    assert context.root_directory.split("/")[-1].startswith(
        "test_ConfigOnlyDataContext__initialization__dir"
    )

    plugins_dir_parts = context.plugins_directory.split("/")[-3:]
    assert len(plugins_dir_parts) == 3
    assert plugins_dir_parts[0].startswith(
        "test_ConfigOnlyDataContext__initialization__dir"
    )
    assert plugins_dir_parts[1:] == ["plugins", ""]


@pytest.mark.unit
def test__normalize_absolute_or_relative_path(
    tmp_path_factory, basic_data_context_config
):
    full_test_dir = tmp_path_factory.mktemp(
        "test__normalize_absolute_or_relative_path__dir"
    )
    test_dir = full_test_dir.parts[-1]
    config_path = str(full_test_dir)
    context = get_context(
        basic_data_context_config,
        config_path,
    )

    assert context._normalize_absolute_or_relative_path("yikes").endswith(
        os.path.join(test_dir, "yikes")  # noqa: PTH118
    )

    assert test_dir not in context._normalize_absolute_or_relative_path("/yikes")
    assert "/yikes" == context._normalize_absolute_or_relative_path("/yikes")


@pytest.mark.filesystem
def test_load_data_context_from_environment_variables(tmp_path, monkeypatch):
    # `find_context_root_dir` iterates up the file tree to find a great_expectations.yml
    # By deeply nesting our project path, we ensure we don't collide with any existing
    # fixtures or side effects from other tests
    project_path = tmp_path / "a" / "b" / "c" / "d" / "data_context"
    project_path.mkdir(parents=True)

    context_path = project_path / "great_expectations"
    context_path.mkdir()
    monkeypatch.chdir(str(context_path))

    with pytest.raises(gx_exceptions.ConfigNotFoundError):
        FileDataContext.find_context_root_dir()

    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..", "test_fixtures", "great_expectations_basic.yml"
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    monkeypatch.setenv("GX_HOME", str(context_path))
    assert FileDataContext.find_context_root_dir() == str(context_path)


@pytest.mark.filesystem
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
        os.path.join(  # noqa: PTH118
            data_context_parameterized_expectation_suite.root_directory,
            "expectations",
            "a_new_new_suite_name.json",
        ),
    ) as suite_file:
        loaded_suite_dict: dict = expectationSuiteSchema.load(json.load(suite_file))
        loaded_suite = ExpectationSuite(
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


@pytest.mark.filesystem
def test_data_context_create_does_not_raise_error_or_warning_if_ge_dir_exists(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    FileDataContext.create(project_path)


@pytest.fixture()
def empty_context(tmp_path_factory) -> FileDataContext:
    project_path = str(tmp_path_factory.mktemp("data_context"))
    FileDataContext.create(project_path)
    ge_dir = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    assert os.path.isdir(ge_dir)  # noqa: PTH112
    assert os.path.isfile(  # noqa: PTH113
        os.path.join(ge_dir, FileDataContext.GX_YML)  # noqa: PTH118
    )
    context = DataContext(ge_dir)
    assert isinstance(context, FileDataContext)
    return context


@pytest.mark.filesystem
def test_data_context_is_project_scaffolded(empty_context):
    ge_dir = empty_context.root_directory
    assert FileDataContext.is_project_scaffolded(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_does_ge_yml_exist_returns_true_when_it_does_exist(empty_context):
    ge_dir = empty_context.root_directory
    assert FileDataContext.does_config_exist_on_disk(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_does_ge_yml_exist_returns_false_when_it_does_not_exist(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(os.path.join(ge_dir, empty_context.GX_YML))  # noqa: PTH118
    assert FileDataContext.does_config_exist_on_disk(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_true_when_it_has_a_datasource_configured_in_yml_file_on_disk(
    empty_context,
):
    ge_dir = empty_context.root_directory
    empty_context.add_datasource("arthur", **{"class_name": "PandasDatasource"})
    assert (
        FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is True
    )


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_datasource_configured_in_yml_file_on_disk(
    empty_context,
):
    ge_dir = empty_context.root_directory
    assert (
        FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False
    )


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_yml_file(
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir, empty_context.GX_YML))  # noqa: PTH118
    assert (
        FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False
    )


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_dir(
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir))  # noqa: PTH118
    assert (
        FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False
    )


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_the_project_has_an_invalid_config_file(
    empty_context,
):
    ge_dir = empty_context.root_directory
    with open(os.path.join(ge_dir, FileDataContext.GX_YML), "w") as yml:  # noqa: PTH118
        yml.write("this file: is not a valid ge config")
    assert (
        FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False
    )


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_one_suite(
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.add_datasource("arthur", class_name="PandasDatasource")
    context.add_expectation_suite("dent")
    assert len(context.list_expectation_suites()) == 1

    assert FileDataContext.is_project_initialized(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_no_suites(
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.add_datasource("arthur", class_name="PandasDatasource")
    assert len(context.list_expectation_suites()) == 0

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_its_valid_context_has_no_datasource(
    empty_context,
):
    ge_dir = empty_context.root_directory
    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_config_yml_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(os.path.join(ge_dir, empty_context.GX_YML))  # noqa: PTH118

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_uncommitted_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(
        os.path.join(ge_dir, empty_context.GX_UNCOMMITTED_DIR)  # noqa: PTH118
    )

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_uncommitted_data_docs_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(
        os.path.join(  # noqa: PTH118
            ge_dir, empty_context.GX_UNCOMMITTED_DIR, "data_docs"
        )
    )

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_uncommitted_validations_dir_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    shutil.rmtree(
        os.path.join(  # noqa: PTH118
            ge_dir, empty_context.GX_UNCOMMITTED_DIR, "validations"
        )
    )

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_false_when_config_variable_yml_is_missing(
    empty_context,
):
    ge_dir = empty_context.root_directory
    # mangle project
    safe_remove(
        os.path.join(  # noqa: PTH118
            ge_dir, empty_context.GX_UNCOMMITTED_DIR, "config_variables.yml"
        )
    )

    assert FileDataContext.is_project_initialized(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_create_raises_warning_and_leaves_existing_yml_untouched(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    FileDataContext.create(project_path)
    ge_yml = os.path.join(  # noqa: PTH118
        project_path, "great_expectations/great_expectations.yml"
    )
    with open(ge_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    with pytest.warns(UserWarning):
        FileDataContext.create(project_path)

    with open(ge_yml) as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


@pytest.mark.filesystem
def test_data_context_create_makes_uncommitted_dirs_when_all_are_missing(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    FileDataContext.create(project_path)

    # mangle the existing setup
    ge_dir = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    shutil.rmtree(uncommitted_dir)

    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        # re-run create to simulate onboarding
        FileDataContext.create(project_path)
    obs = gen_directory_tree_str(ge_dir)

    assert os.path.isdir(  # noqa: PTH112
        uncommitted_dir
    ), "No uncommitted directory created"
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


@pytest.mark.filesystem
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
    ge_dir = os.path.join(project_path, "great_expectations")  # noqa: PTH118

    FileDataContext.create(project_path)
    fixture = gen_directory_tree_str(ge_dir)

    assert fixture == expected

    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        # re-run create to simulate onboarding
        FileDataContext.create(project_path)

    obs = gen_directory_tree_str(ge_dir)
    assert obs == expected


@pytest.mark.filesystem
def test_data_context_do_all_uncommitted_dirs_exist(tmp_path_factory):
    expected = """\
uncommitted/
    config_variables.yml
    data_docs/
    validations/
        .ge_store_backend_id
"""
    project_path = str(tmp_path_factory.mktemp("stuff"))
    ge_dir = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    FileDataContext.create(project_path)
    fixture = gen_directory_tree_str(uncommitted_dir)
    assert fixture == expected

    # Test that all exist
    assert FileDataContext.all_uncommitted_directories_exist(ge_dir)

    # remove a few
    shutil.rmtree(os.path.join(uncommitted_dir, "data_docs"))  # noqa: PTH118
    shutil.rmtree(os.path.join(uncommitted_dir, "validations"))  # noqa: PTH118

    # Test that not all exist
    assert not FileDataContext.all_uncommitted_directories_exist(project_path)


@pytest.mark.filesystem
def test_data_context_create_builds_base_directories(tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context = FileDataContext.create(project_path)
    assert isinstance(context, FileDataContext)

    for directory in [
        "expectations",
        "plugins",
        "profilers",
        "checkpoints",
        "uncommitted",
    ]:
        base_dir = os.path.join(project_path, context.GX_DIR, directory)  # noqa: PTH118
        assert os.path.isdir(base_dir)  # noqa: PTH112


@pytest.mark.filesystem
def test_data_context_create_does_not_overwrite_existing_config_variables_yml(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    FileDataContext.create(project_path)
    ge_dir = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    config_vars_yml = os.path.join(  # noqa: PTH118
        uncommitted_dir, "config_variables.yml"
    )

    # modify config variables
    with open(config_vars_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    # re-run create to simulate onboarding
    with pytest.warns(UserWarning):
        FileDataContext.create(project_path)

    with open(config_vars_yml) as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


@pytest.mark.filesystem
def test_scaffold_directories(tmp_path_factory):
    empty_directory = str(tmp_path_factory.mktemp("test_scaffold_directories"))
    FileDataContext._scaffold_directories(empty_directory)

    assert set(os.listdir(empty_directory)) == {
        "plugins",
        "checkpoints",
        "profilers",
        "expectations",
        ".gitignore",
        "uncommitted",
    }
    assert set(
        os.listdir(os.path.join(empty_directory, "uncommitted"))  # noqa: PTH118
    ) == {
        "data_docs",
        "validations",
    }


@pytest.mark.filesystem
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
    paths.append(os.path.basename(batch_kwargs["path"]))  # noqa: PTH119

    batch_kwargs = titanic_multibatch_data_context.build_batch_kwargs(
        "mydatasource", "mygenerator", data_asset_name="titanic"
    )
    paths.append(os.path.basename(batch_kwargs["path"]))  # noqa: PTH119

    assert {"Titanic_1912.csv", "Titanic_1911.csv"} == set(paths)


@pytest.mark.filesystem
def test_load_config_variables_property(
    basic_data_context_config, tmp_path_factory, monkeypatch
):
    # Setup:
    base_path = str(tmp_path_factory.mktemp("test_load_config_variables_file"))
    os.makedirs(  # noqa: PTH103
        os.path.join(base_path, "uncommitted"), exist_ok=True  # noqa: PTH118
    )
    with open(
        os.path.join(base_path, "uncommitted", "dev_variables.yml"), "w"  # noqa: PTH118
    ) as outfile:
        yaml.dump({"env": "dev"}, outfile)
    with open(
        os.path.join(base_path, "uncommitted", "prod_variables.yml"),  # noqa: PTH118
        "w",
    ) as outfile:
        yaml.dump({"env": "prod"}, outfile)
    basic_data_context_config[
        "config_variables_file_path"
    ] = "uncommitted/${TEST_CONFIG_FILE_ENV}_variables.yml"

    try:
        # We should be able to load different files based on an environment variable
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "dev")
        context = get_context(basic_data_context_config, context_root_dir=base_path)
        config_vars = context.config_variables
        assert config_vars["env"] == "dev"
        monkeypatch.setenv("TEST_CONFIG_FILE_ENV", "prod")
        context = get_context(basic_data_context_config, context_root_dir=base_path)
        config_vars = context.config_variables
        assert config_vars["env"] == "prod"
    except Exception:
        raise
    finally:
        # Make sure we unset the environment variable we're using
        monkeypatch.delenv("TEST_CONFIG_FILE_ENV")


@pytest.mark.unit
def test_list_expectation_suite_with_no_suites(titanic_data_context):
    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == []


@pytest.mark.unit
def test_list_expectation_suite_with_one_suite(titanic_data_context):
    titanic_data_context.add_expectation_suite("warning")
    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == ["warning"]


@pytest.mark.unit
def test_list_expectation_suite_with_multiple_suites(titanic_data_context):
    titanic_data_context.add_expectation_suite("a.warning")
    titanic_data_context.add_expectation_suite("b.warning")
    titanic_data_context.add_expectation_suite("c.warning")

    observed = titanic_data_context.list_expectation_suite_names()
    assert isinstance(observed, list)
    assert observed == ["a.warning", "b.warning", "c.warning"]
    assert len(observed) == 3


@pytest.mark.unit
def test_get_batch_raises_error_when_passed_a_non_string_type_for_suite_parameter(
    titanic_data_context,
):
    with pytest.raises(gx_exceptions.DataContextError):
        titanic_data_context.get_batch({}, 99)


@pytest.mark.unit
def test_get_batch_raises_error_when_passed_a_non_dict_or_batch_kwarg_type_for_batch_kwarg_parameter(
    titanic_data_context,
):
    with pytest.raises(gx_exceptions.BatchKwargsError):
        titanic_data_context.get_batch(99, "foo")


@pytest.mark.filesystem
def test_get_batch_when_passed_a_suite_name(titanic_data_context):
    context = titanic_data_context
    root_dir = context.root_directory
    batch_kwargs = {
        "datasource": "mydatasource",
        "path": os.path.join(root_dir, "..", "data", "Titanic.csv"),  # noqa: PTH118
    }
    context.add_expectation_suite("foo")
    assert context.list_expectation_suite_names() == ["foo"]
    batch = context.get_batch(batch_kwargs, "foo")
    assert isinstance(batch, Dataset)
    assert isinstance(batch.get_expectation_suite(), ExpectationSuite)


@pytest.mark.filesystem
def test_get_batch_when_passed_a_suite(titanic_data_context):
    context = titanic_data_context
    root_dir = context.root_directory
    batch_kwargs = {
        "datasource": "mydatasource",
        "path": os.path.join(root_dir, "..", "data", "Titanic.csv"),  # noqa: PTH118
    }
    context.add_expectation_suite("foo")
    assert context.list_expectation_suite_names() == ["foo"]
    suite = context.get_expectation_suite("foo")

    batch = context.get_batch(batch_kwargs, suite)
    assert isinstance(batch, Dataset)
    assert isinstance(batch.get_expectation_suite(), ExpectationSuite)


@pytest.mark.unit
def test_list_validation_operators_data_context_with_none_returns_empty_list(
    titanic_data_context,
):
    titanic_data_context.validation_operators = {}
    assert titanic_data_context.list_validation_operator_names() == []


@pytest.mark.unit
def test_list_validation_operators_data_context_with_one(titanic_data_context):
    assert titanic_data_context.list_validation_operator_names() == [
        "action_list_operator"
    ]


@pytest.mark.unit
def test_list_checkpoints_on_empty_context_returns_empty_list(empty_data_context):
    assert empty_data_context.list_checkpoints() == []


@pytest.mark.unit
def test_list_checkpoints_on_context_with_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    assert context.list_checkpoints() == ["my_checkpoint"]


@pytest.mark.filesystem
def test_list_checkpoints_on_context_with_two_checkpoints(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    checkpoints_file = os.path.join(  # noqa: PTH118
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_checkpoint.yml",
    )
    shutil.copy(
        checkpoints_file,
        os.path.join(  # noqa: PTH118
            os.path.dirname(checkpoints_file), "another.yml"  # noqa: PTH120
        ),
    )
    assert set(context.list_checkpoints()) == {"another", "my_checkpoint"}


@pytest.mark.filesystem
def test_list_checkpoints_on_context_with_checkpoint_and_other_files_in_checkpoints_dir(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint

    for extension in [".json", ".txt", "", ".py"]:
        path = os.path.join(  # noqa: PTH118
            context.root_directory,
            DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
            f"foo{extension}",
        )
        with open(path, "w") as f:
            f.write("foo: bar")
        assert os.path.isfile(path)  # noqa: PTH113

    assert context.list_checkpoints() == ["my_checkpoint"]


@pytest.mark.unit
def test_get_checkpoint_raises_error_on_not_found_checkpoint(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    with pytest.raises(gx_exceptions.CheckpointNotFoundError):
        context.get_checkpoint("not_a_checkpoint")


@pytest.mark.filesystem
def test_get_checkpoint_raises_error_empty_checkpoint(
    empty_context_with_checkpoint,
):
    context = empty_context_with_checkpoint
    checkpoint_file_path = os.path.join(  # noqa: PTH118
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_checkpoint.yml",
    )
    with open(checkpoint_file_path, "w") as f:
        f.write("# Not a Checkpoint file")
    assert os.path.isfile(checkpoint_file_path)  # noqa: PTH113
    assert context.list_checkpoints() == ["my_checkpoint"]

    with pytest.raises(gx_exceptions.InvalidCheckpointConfigError):
        context.get_checkpoint("my_checkpoint")


@pytest.mark.unit
def test_get_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    obs = context.get_checkpoint("my_checkpoint")
    assert isinstance(obs, Checkpoint)
    config = obs.get_config(mode=ConfigOutputModes.JSON_DICT)
    assert isinstance(config, dict)
    assert sorted(config.keys()) == [
        "action_list",
        "batch_request",
        "class_name",
        "config_version",
        "evaluation_parameters",
        "module_name",
        "name",
        "profilers",
        "run_name_template",
        "runtime_configuration",
        "validations",
    ]


@pytest.mark.big
@pytest.mark.integration
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
        gx_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        context.run_checkpoint(checkpoint_name=checkpoint_config.name)

    assert len(context.validations_store.list_keys()) == 0

    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

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


@pytest.mark.filesystem
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


@pytest.mark.filesystem
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


@pytest.mark.big
@pytest.mark.slow  # 8.13s
def test_get_validator_without_expectation_suite(in_memory_runtime_context):
    context = in_memory_runtime_context

    batch = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="my_data_asset",
            runtime_parameters={"batch_data": pd.DataFrame({"x": range(10)})},
            batch_identifiers={
                "id_key_0": "id_0_value_a",
                "id_key_1": "id_1_value_a",
            },
        )
    )[0]

    my_validator = context.get_validator(batch=batch)
    assert isinstance(my_validator.get_expectation_suite(), ExpectationSuite)
    assert my_validator.expectation_suite_name == "default"


@pytest.mark.big
@pytest.mark.slow  # 1.35s
def test_get_validator_with_batch(in_memory_runtime_context):
    context = in_memory_runtime_context

    my_batch = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="my_data_asset",
            runtime_parameters={"batch_data": pd.DataFrame({"x": range(10)})},
            batch_identifiers={
                "id_key_0": "id_0_value_a",
                "id_key_1": "id_1_value_a",
            },
        )
    )[0]

    context.get_validator(
        batch=my_batch,
        create_expectation_suite_with_name="A_expectation_suite",
    )


@pytest.mark.big
def test_get_validator_with_batch_list(in_memory_runtime_context):
    context = in_memory_runtime_context

    my_batch_list = [
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={"batch_data": pd.DataFrame({"x": range(10)})},
                batch_identifiers={
                    "id_key_0": "id_0_value_a",
                    "id_key_1": "id_1_value_a",
                },
            )
        )[0],
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="my_data_asset",
                runtime_parameters={"batch_data": pd.DataFrame({"y": range(10)})},
                batch_identifiers={
                    "id_key_0": "id_0_value_b",
                    "id_key_1": "id_1_value_b",
                },
            )
        )[0],
    ]

    my_validator = context.get_validator(
        batch_list=my_batch_list,
        create_expectation_suite_with_name="A_expectation_suite",
    )
    assert len(my_validator.batches) == 2


@pytest.mark.filesystem
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
    context.add_expectation_suite(expectation_suite_name="local_test.default")
    expectation_suite = context.get_expectation_suite("local_test.default")
    context.add_datasource("pandas_datasource", class_name="PandasDatasource")
    df = pd.DataFrame({"a": [1, 2, 3]})
    batch = context.get_batch(
        batch_kwargs={"datasource": "pandas_datasource", "dataset": df},
        expectation_suite_name=expectation_suite,
    )
    assert len(batch) == 3


@pytest.mark.filesystem
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_expectation_to_expectation_suite(
    mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    expectation_suite: ExpectationSuite = context.add_expectation_suite(
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


@pytest.mark.filesystem
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
class_name: SimpleCheckpoint
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

    checkpoint_dir = os.path.join(  # noqa: PTH118
        context.root_directory,
        context.checkpoint_store.config["store_backend"]["base_directory"],
    )
    checkpoint_file = os.path.join(  # noqa: PTH118
        checkpoint_dir, f"{checkpoint_name}.yml"
    )

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
            "action": {"class_name": "UpdateDataDocsAction"},
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
        "class_name": "SimpleCheckpoint",
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
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
        "validations": [
            {
                "name": None,
                "id": None,
                "expectation_suite_name": "newsuite",
                "expectation_suite_ge_cloud_id": None,
                "batch_request": {
                    "datasource_name": "data_dir",
                    "data_connector_name": "data_dir_example_data_connector",
                    "data_asset_name": "DEFAULT_ASSET_NAME",
                    "partition_request": {"index": -1},
                },
            }
        ],
    }

    assert isinstance(checkpoint_from_yaml, Checkpoint)

    assert checkpoint_name in context.list_checkpoints()
    assert len(context.list_checkpoints()) == 1

    # No other usage stats calls detected
    assert mock_emit.call_count == 1


@pytest.mark.filesystem
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


@pytest.mark.filesystem
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
                "name": "data_dir_example_data_connector",
            }
        },
        "id": None,
        "name": "my_datasource",
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


@pytest.mark.filesystem
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_add_datasource_from_yaml_sql_datasource(  # noqa: PLR0915
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


@pytest.mark.filesystem
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
                "name": "default_inferred_data_connector_name",
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
                "module_name": "great_expectations.datasource.data_connector",
                "name": "default_runtime_data_connector_name",
            },
        },
        "id": None,
        "name": "my_datasource",
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
                "name": "default_inferred_data_connector_name",
            },
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
                "module_name": "great_expectations.datasource.data_connector",
                "name": "default_runtime_data_connector_name",
            },
        },
        "id": None,
        "name": "my_datasource",
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


@pytest.mark.filesystem
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
                "name": "data_dir_example_data_connector",
            }
        },
        "id": None,
        "name": "my_datasource",
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


@pytest.mark.filesystem
def test_stores_evaluation_parameters_resolve_correctly(data_context_with_query_store):
    """End to end test demonstrating usage of Stores evaluation parameters"""
    context = data_context_with_query_store
    suite_name = "eval_param_suite"
    context.add_expectation_suite(expectation_suite_name=suite_name)
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


@pytest.mark.filesystem
def test_modifications_to_env_vars_is_recognized_within_same_program_execution(
    empty_data_context: DataContext, monkeypatch
) -> None:
    """
    What does this test do and why?

    Great Expectations recognizes changes made to environment variables within a program execution
    and ensures that these changes are recognized in subsequent calls within the same process.

    This is particularly relevant when performing substitutions within a user's project config.
    """
    context: DataContext = empty_data_context
    env_var_name: str = "MY_PLUGINS_DIRECTORY"
    env_var_value: str = "my_patched_value"

    context.variables.config.plugins_directory = f"${env_var_name}"
    monkeypatch.setenv(env_var_name, env_var_value)

    assert context.plugins_directory and context.plugins_directory.endswith(
        env_var_value
    )


@pytest.mark.filesystem
def test_modifications_to_config_vars_is_recognized_within_same_program_execution(
    empty_data_context: DataContext,
) -> None:
    """
    What does this test do and why?

    Great Expectations recognizes changes made to config variables within a program execution
    and ensures that these changes are recognized in subsequent calls within the same process.

    This is particularly relevant when performing substitutions within a user's project config.
    """
    context: DataContext = empty_data_context
    config_var_name: str = "my_plugins_dir"
    config_var_value: str = "my_patched_value"

    context.variables.config.plugins_directory = f"${config_var_name}"
    context.save_config_variable(
        config_variable_name=config_var_name, value=config_var_value
    )

    assert context.plugins_directory and context.plugins_directory.endswith(
        config_var_value
    )


@pytest.mark.big
@pytest.mark.integration
def test_check_for_usage_stats_sync_finds_diff(
    empty_data_context_stats_enabled: DataContext,
    data_context_config_with_datasources: DataContextConfig,
) -> None:
    """
    What does this test do and why?

    During DataContext instantiation, if the project config used to create the object
    and the actual config assigned to self.config differ in terms of their usage statistics
    values, we want to be able to identify that and persist values accordingly.
    """
    context = empty_data_context_stats_enabled
    project_config = data_context_config_with_datasources

    res = context._check_for_usage_stats_sync(project_config=project_config)
    assert res is True


@pytest.mark.big
@pytest.mark.integration
def test_check_for_usage_stats_sync_does_not_find_diff(
    empty_data_context_stats_enabled: DataContext,
) -> None:
    """
    What does this test do and why?

    During DataContext instantiation, if the project config used to create the object
    and the actual config assigned to self.config differ in terms of their usage statistics
    values, we want to be able to identify that and persist values accordingly.
    """
    context = empty_data_context_stats_enabled
    project_config = copy.deepcopy(context.config)  # Using same exact config

    res = context._check_for_usage_stats_sync(project_config=project_config)
    assert res is False


@pytest.mark.big
@pytest.mark.integration
def test_check_for_usage_stats_sync_short_circuits_due_to_disabled_usage_stats(
    empty_data_context: DataContext,
    data_context_config_with_datasources: DataContextConfig,
) -> None:
    context = empty_data_context
    project_config = data_context_config_with_datasources
    project_config.anonymous_usage_statistics.enabled = False

    res = context._check_for_usage_stats_sync(project_config=project_config)
    assert res is False


class ExpectSkyToBeColor(BatchExpectation):
    metric_dependencies = ("table.color",)
    success_keys = ("color",)
    args_keys = ("color",)

    @classmethod
    @renderer(
        renderer_type=".".join(
            [AtomicRendererType.PRESCRIPTIVE, "custom_renderer_type"]
        )
    )
    def _prescriptive_renderer_custom(
        cls,
        **kwargs: dict,
    ) -> None:
        raise ValueError("This renderer is broken!")

    def _validate(
        self,
        **kwargs: dict,
    ) -> Dict[str, Union[bool, dict]]:
        return {
            "success": True,
            "result": {"observed_value": "blue"},
        }


@pytest.mark.filesystem
@pytest.mark.integration
def test_unrendered_and_failed_prescriptive_renderer_behavior(
    empty_data_context: DataContext,
):
    context: DataContext = empty_data_context

    expectation_suite_name: str = "test_suite"

    expectation_suite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name,
        data_context=context,
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_equal", kwargs={"value": 0}
            ),
        ],
    )
    context.add_expectation_suite(expectation_suite=expectation_suite)

    # Without include_rendered_content set, all legacy rendered_content was None.
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert not any(
        expectation_configuration.rendered_content
        for expectation_configuration in expectation_suite.expectations
    )

    # Once we include_rendered_content, we get rendered_content on each ExpectationConfiguration in the ExpectationSuite.
    context.variables.include_rendered_content.expectation_suite = True
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    for expectation_configuration in expectation_suite.expectations:
        assert all(
            isinstance(rendered_content_block, RenderedAtomicContent)
            for rendered_content_block in expectation_configuration.rendered_content
        )

    # If we change the ExpectationSuite to use an Expectation that has two content block renderers, one of which is
    # broken, we should get the failure message for one of the content blocks.
    expectation_suite = ExpectationSuite(
        expectation_suite_name=expectation_suite_name,
        data_context=context,
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_sky_to_be_color", kwargs={"color": "blue"}
            ),
        ],
    )
    context.update_expectation_suite(expectation_suite=expectation_suite)
    expectation_suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )

    expected_rendered_content: List[RenderedAtomicContent] = [
        RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.FAILED,
            value=renderedAtomicValueSchema.load(
                {
                    "template": "Rendering failed for Expectation: $expectation_type(**$kwargs).",
                    "params": {
                        "expectation_type": {
                            "schema": {"type": "string"},
                            "value": "expect_sky_to_be_color",
                        },
                        "kwargs": {
                            "schema": {"type": "string"},
                            "value": {"color": "blue"},
                        },
                    },
                    "schema": {"type": "com.superconductive.rendered.string"},
                }
            ),
            value_type="StringValueType",
            exception='Renderer "atomic.prescriptive.custom_renderer_type" failed to render Expectation '
            '"expect_sky_to_be_color with exception message: This renderer is broken!".',
        ),
        RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.SUMMARY,
            value=renderedAtomicValueSchema.load(
                {
                    "schema": {"type": "com.superconductive.rendered.string"},
                    "template": "$expectation_type(**$kwargs)",
                    "params": {
                        "expectation_type": {
                            "schema": {"type": "string"},
                            "value": "expect_sky_to_be_color",
                        },
                        "kwargs": {
                            "schema": {"type": "string"},
                            "value": {"color": "blue"},
                        },
                    },
                }
            ),
            value_type="StringValueType",
        ),
    ]

    actual_rendered_content: List[RenderedAtomicContent] = []
    for expectation_configuration in expectation_suite.expectations:
        actual_rendered_content.extend(expectation_configuration.rendered_content)

    assert actual_rendered_content == expected_rendered_content

    # If we have a legacy ExpectationSuite with successful rendered_content blocks, but the new renderer is broken,
    # we should not update the existing rendered_content.
    legacy_rendered_content = [
        RenderedAtomicContent(
            name=".".join([AtomicRendererType.PRESCRIPTIVE, "custom_renderer_type"]),
            value=renderedAtomicValueSchema.load(
                {
                    "schema": {"type": "com.superconductive.rendered.string"},
                    "template": "This is a working renderer for $expectation_type(**$kwargs).",
                    "params": {
                        "expectation_type": {
                            "schema": {"type": "string"},
                            "value": "expect_sky_to_be_color",
                        },
                        "kwargs": {
                            "schema": {"type": "string"},
                            "value": {"color": "blue"},
                        },
                    },
                }
            ),
            value_type="StringValueType",
        ),
        RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.SUMMARY,
            value=renderedAtomicValueSchema.load(
                {
                    "schema": {"type": "com.superconductive.rendered.string"},
                    "template": "$expectation_type(**$kwargs)",
                    "params": {
                        "expectation_type": {
                            "schema": {"type": "string"},
                            "value": "expect_sky_to_be_color",
                        },
                        "kwargs": {
                            "schema": {"type": "string"},
                            "value": {"color": "blue"},
                        },
                    },
                }
            ),
            value_type="StringValueType",
        ),
    ]

    expectation_suite.expectations[0].rendered_content = legacy_rendered_content

    actual_rendered_content: List[RenderedAtomicContent] = []
    for expectation_configuration in expectation_suite.expectations:
        actual_rendered_content.extend(expectation_configuration.rendered_content)

    assert actual_rendered_content == legacy_rendered_content
