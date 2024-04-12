from __future__ import annotations

import configparser
import copy
import json
import os
import pathlib
import shutil
import uuid
from typing import Dict, List, Union

import pandas as pd
import pytest
from typing_extensions import override

import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    expectationSuiteSchema,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import get_context
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    DataContextConfigDefaults,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import (
    Datasource,
    LegacyDatasource,
)
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render import (
    AtomicPrescriptiveRendererType,
    AtomicRendererType,
    RenderedAtomicContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.util import (
    gen_directory_tree_str,
)
from tests.test_utils import create_files_in_directory, safe_remove

try:
    from unittest import mock
except ImportError:
    from unittest import mock

yaml = YAMLHandler()

parameterized_expectation_suite_name = "my_dag_node.default"


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
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
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
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
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
    # create expectation suite with name that already exists on data asset, but pass overwrite_existing=True  # noqa: E501
    assert titanic_data_context.add_or_update_expectation_suite(
        expectation_suite_name="titanic.test_create_expectation_suite",
    )


@pytest.mark.filesystem
def test_list_expectation_suite_keys(data_context_parameterized_expectation_suite):
    assert data_context_parameterized_expectation_suite.list_expectation_suites() == [
        ExpectationSuiteIdentifier(name=parameterized_expectation_suite_name)
    ]


@pytest.mark.filesystem
def test_get_existing_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        parameterized_expectation_suite_name
    )
    assert expectation_suite.name == parameterized_expectation_suite_name
    assert len(expectation_suite.expectations) == 2


@pytest.mark.filesystem
def test_get_new_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.add_expectation_suite(
        "this_data_asset_does_not_exist.default"
    )
    assert expectation_suite.name == "this_data_asset_does_not_exist.default"
    assert len(expectation_suite.expectations) == 0


@pytest.mark.filesystem
def test_save_expectation_suite(data_context_parameterized_expectation_suite):
    expectation_suite = data_context_parameterized_expectation_suite.add_expectation_suite(
        "this_data_asset_config_does_not_exist.default"
    )
    expectation_suite.expectation_configurations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    data_context_parameterized_expectation_suite.save_expectation_suite(expectation_suite)
    expectation_suite_saved = data_context_parameterized_expectation_suite.get_expectation_suite(
        "this_data_asset_config_does_not_exist.default"
    )
    assert (
        expectation_suite.expectation_configurations
        == expectation_suite_saved.expectation_configurations
    )


@pytest.mark.filesystem
def test_save_expectation_suite_include_rendered_content(
    data_context_parameterized_expectation_suite,
):
    expectation_suite: ExpectationSuite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    expectation_suite.expectation_configurations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    for expectation in expectation_suite.expectation_configurations:
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
    for expectation in expectation_suite_saved.expectation_configurations:
        for rendered_content_block in expectation.rendered_content:
            assert isinstance(
                rendered_content_block,
                RenderedAtomicContent,
            )


@pytest.mark.filesystem
def test_get_expectation_suite_include_rendered_content(
    data_context_parameterized_expectation_suite,
):
    expectation_suite: ExpectationSuite = (
        data_context_parameterized_expectation_suite.add_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    expectation_suite.expectation_configurations.append(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )
    for expectation in expectation_suite.expectation_configurations:
        assert expectation.rendered_content is None
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite,
    )
    (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default"
        )
    )
    for expectation in expectation_suite.expectation_configurations:
        assert expectation.rendered_content is None

    expectation_suite_retrieved: ExpectationSuite = (
        data_context_parameterized_expectation_suite.get_expectation_suite(
            "this_data_asset_config_does_not_exist.default",
            include_rendered_content=True,
        )
    )

    for expectation in expectation_suite_retrieved.expectation_configurations:
        for rendered_content_block in expectation.rendered_content:
            assert isinstance(
                rendered_content_block,
                RenderedAtomicContent,
            )


@pytest.mark.filesystem
def test_compile_suite_parameter_dependencies(
    data_context_parameterized_expectation_suite,
):
    assert data_context_parameterized_expectation_suite._suite_parameter_dependencies == {}
    data_context_parameterized_expectation_suite._compile_suite_parameter_dependencies()
    assert data_context_parameterized_expectation_suite._suite_parameter_dependencies == {
        "source_diabetes_data.default": [
            {
                "metric_kwargs_id": {
                    "column=patient_nbr": [
                        "expect_column_unique_value_count_to_be_between.result.observed_value"
                    ]
                }
            }
        ],
        "source_patient_data.default": ["expect_table_row_count_to_equal.result.observed_value"],
    }


@pytest.mark.filesystem
def test_compile_suite_parameter_dependencies_broken_suite(
    data_context_parameterized_expectation_suite: FileDataContext,
):
    broken_suite_path = pathlib.Path(
        data_context_parameterized_expectation_suite.root_directory,
        "expectations",
        "broken_suite.json",
    )
    broken_suite_dict = {
        "expectation_suite_name": "broken suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "col_1",
                    "max_value": 5,
                    "min_value": 1,
                    "mostly": 0.5,
                },
                "meta": {},
                "not_a_valid_expectation_config_arg": "break it!",
            },
        ],
        "meta": {"great_expectations_version": "0.18.8"},
    }
    with broken_suite_path.open("w", encoding="UTF-8") as fp:
        json.dump(obj=broken_suite_dict, fp=fp)

    assert data_context_parameterized_expectation_suite._suite_parameter_dependencies == {}
    with pytest.warns(UserWarning):
        data_context_parameterized_expectation_suite._compile_suite_parameter_dependencies()
    assert data_context_parameterized_expectation_suite._suite_parameter_dependencies == {
        "source_diabetes_data.default": [
            {
                "metric_kwargs_id": {
                    "column=patient_nbr": [
                        "expect_column_unique_value_count_to_be_between.result.observed_value"
                    ]
                }
            }
        ],
        "source_patient_data.default": ["expect_table_row_count_to_equal.result.observed_value"],
    }


@pytest.mark.filesystem
@mock.patch("great_expectations.data_context.store.DatasourceStore.update_by_name")
def test_update_datasource_persists_changes_with_store(
    mock_update_by_name: mock.MagicMock,  # noqa: TID251
    data_context_parameterized_expectation_suite,
) -> None:
    context = data_context_parameterized_expectation_suite

    datasource_to_update: Datasource = tuple(context.datasources.values())[0]

    context.update_datasource(datasource=datasource_to_update)

    assert mock_update_by_name.call_count == 1


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
    empty_data_context.delete_expectation_suite(expectation_suite_name=expectation_suites[0])
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
    empty_data_context.delete_expectation_suite(expectation_suite_name=expectation_suites[0])
    expectation_suites = empty_data_context.list_expectation_suite_names()
    assert len(expectation_suites) == 1


@pytest.mark.unit
def test_data_context_get_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context,
):
    with pytest.raises(ValueError):
        _ = titanic_data_context.get_datasource("fakey_mc_fake")


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
def test_ConfigOnlyDataContext__initialization(tmp_path_factory, basic_data_context_config):
    config_path = str(tmp_path_factory.mktemp("test_ConfigOnlyDataContext__initialization__dir"))
    context = get_context(
        basic_data_context_config,
        config_path,
    )

    assert context.root_directory.split("/")[-1].startswith(
        "test_ConfigOnlyDataContext__initialization__dir"
    )

    plugins_dir_parts = context.plugins_directory.split("/")[-3:]
    assert len(plugins_dir_parts) == 3
    assert plugins_dir_parts[0].startswith("test_ConfigOnlyDataContext__initialization__dir")
    assert plugins_dir_parts[1:] == ["plugins", ""]


@pytest.mark.unit
def test__normalize_absolute_or_relative_path(tmp_path_factory, basic_data_context_config):
    full_test_dir = tmp_path_factory.mktemp("test__normalize_absolute_or_relative_path__dir")
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

    context_path = project_path / FileDataContext.GX_DIR
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
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    monkeypatch.setenv("GX_HOME", str(context_path))
    assert FileDataContext.find_context_root_dir() == str(context_path)


@pytest.mark.filesystem
def test_data_context_updates_expectation_suite_names(
    data_context_parameterized_expectation_suite,
):
    # A data context should update the data_asset_name and expectation_suite_name of expectation suites  # noqa: E501
    # that it creates when it saves them.

    expectation_suites = data_context_parameterized_expectation_suite.list_expectation_suites()

    # We should have a single expectation suite defined
    assert len(expectation_suites) == 1

    expectation_suite_name = expectation_suites[0].name

    # We'll get that expectation suite and then update its name and re-save, then verify that everything  # noqa: E501
    # has been properly updated
    expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        expectation_suite_name
    )

    # Note we codify here the current behavior of having a string data_asset_name though typed ExpectationSuite objects  # noqa: E501
    # will enable changing that
    assert expectation_suite.name == expectation_suite_name

    # We will now change the data_asset_name and then save the suite in three ways:
    #   1. Directly using the new name,
    #   2. Using a different name that should be overwritten
    #   3. Using the new name but having the context draw that from the suite

    # Finally, we will try to save without a name (deleting it first) to demonstrate that saving will fail.  # noqa: E501

    expectation_suite.name = "a_new_suite_name"

    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite, expectation_suite_name="a_new_suite_name"
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_new_suite_name"
    )

    assert fetched_expectation_suite.name == "a_new_suite_name"

    #   2. Using a different name that should be overwritten
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite,
        expectation_suite_name="a_new_new_suite_name",
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_new_new_suite_name"
    )

    assert fetched_expectation_suite.name == "a_new_new_suite_name"

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
        )
        assert loaded_suite.name == "a_new_new_suite_name"

    #   3. Using the new name but having the context draw that from the suite
    expectation_suite.name = "a_third_suite_name"
    data_context_parameterized_expectation_suite.save_expectation_suite(
        expectation_suite=expectation_suite
    )

    fetched_expectation_suite = data_context_parameterized_expectation_suite.get_expectation_suite(
        "a_third_suite_name"
    )
    assert fetched_expectation_suite.name == "a_third_suite_name"


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
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    assert os.path.isdir(ge_dir)  # noqa: PTH112
    assert os.path.isfile(  # noqa: PTH113
        os.path.join(ge_dir, FileDataContext.GX_YML)  # noqa: PTH118
    )
    context = get_context(context_root_dir=ge_dir)
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
def test_data_context_does_project_have_a_datasource_in_config_file_returns_true_when_it_has_a_datasource_configured_in_yml_file_on_disk(  # noqa: E501
    empty_context,
):
    ge_dir = empty_context.root_directory
    empty_context.add_datasource("arthur", **{"class_name": "PandasDatasource"})
    assert FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_datasource_configured_in_yml_file_on_disk(  # noqa: E501
    empty_context,
):
    ge_dir = empty_context.root_directory
    assert FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_yml_file(  # noqa: E501
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir, empty_context.GX_YML))  # noqa: PTH118
    assert FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_it_does_not_have_a_ge_dir(  # noqa: E501
    empty_context,
):
    ge_dir = empty_context.root_directory
    safe_remove(os.path.join(ge_dir))  # noqa: PTH118
    assert FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_does_project_have_a_datasource_in_config_file_returns_false_when_the_project_has_an_invalid_config_file(  # noqa: E501
    empty_context,
):
    ge_dir = empty_context.root_directory
    with open(os.path.join(ge_dir, FileDataContext.GX_YML), "w") as yml:  # noqa: PTH118
        yml.write("this file: is not a valid ge config")
    assert FileDataContext._does_project_have_a_datasource_in_config_file(ge_dir) is False


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_one_suite(  # noqa: E501
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.add_datasource("arthur", class_name="PandasDatasource")
    context.add_expectation_suite("dent")
    assert len(context.list_expectation_suites()) == 1

    assert FileDataContext.is_project_initialized(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_no_suites(  # noqa: E501
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
def test_data_context_is_project_initialized_returns_false_when_uncommitted_data_docs_dir_is_missing(  # noqa: E501
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
def test_data_context_is_project_initialized_returns_false_when_uncommitted_validations_dir_is_missing(  # noqa: E501
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
    ge_yml = os.path.join(project_path, "gx/great_expectations.yml")  # noqa: PTH118
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
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    shutil.rmtree(uncommitted_dir)

    with pytest.warns(UserWarning, match="Warning. An existing `great_expectations.yml` was found"):
        # re-run create to simulate onboarding
        FileDataContext.create(project_path)
    obs = gen_directory_tree_str(ge_dir)

    assert os.path.isdir(  # noqa: PTH112
        uncommitted_dir
    ), "No uncommitted directory created"
    assert (
        obs
        == """\
gx/
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
    validation_definitions/
"""
    )


@pytest.mark.filesystem
def test_data_context_create_does_nothing_if_all_uncommitted_dirs_exist(
    tmp_path_factory,
):
    expected = """\
gx/
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
    validation_definitions/
"""
    project_path = str(tmp_path_factory.mktemp("stuff"))
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118

    FileDataContext.create(project_path)
    fixture = gen_directory_tree_str(ge_dir)

    assert fixture == expected

    with pytest.warns(UserWarning, match="Warning. An existing `great_expectations.yml` was found"):
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
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
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
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
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
    empty_directory = pathlib.Path(empty_directory)
    FileDataContext._scaffold_directories(empty_directory)

    assert set(os.listdir(empty_directory)) == {
        "plugins",
        "checkpoints",
        "profilers",
        "expectations",
        ".gitignore",
        "uncommitted",
        "validation_definitions",
    }
    assert set(
        os.listdir(os.path.join(empty_directory, "uncommitted"))  # noqa: PTH118
    ) == {
        "data_docs",
        "validations",
    }


@pytest.mark.filesystem
def test_load_config_variables_property(basic_data_context_config, tmp_path_factory, monkeypatch):
    # Setup:
    base_path = str(tmp_path_factory.mktemp("test_load_config_variables_file"))
    os.makedirs(  # noqa: PTH103
        os.path.join(base_path, "uncommitted"),  # noqa: PTH118
        exist_ok=True,
    )
    with open(
        os.path.join(base_path, "uncommitted", "dev_variables.yml"),  # noqa: PTH118
        "w",
    ) as outfile:
        yaml.dump({"env": "dev"}, outfile)
    with open(
        os.path.join(base_path, "uncommitted", "prod_variables.yml"),  # noqa: PTH118
        "w",
    ) as outfile:
        yaml.dump({"env": "prod"}, outfile)
    basic_data_context_config["config_variables_file_path"] = (
        "uncommitted/${TEST_CONFIG_FILE_ENV}_variables.yml"
    )

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
    except Exception:  # noqa: TRY302
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
def test_list_validation_operators_data_context_with_none_returns_empty_list(
    titanic_data_context,
):
    titanic_data_context.validation_operators = {}
    assert titanic_data_context.list_validation_operator_names() == []


@pytest.mark.unit
def test_list_validation_operators_data_context_with_one(titanic_data_context):
    assert titanic_data_context.list_validation_operator_names() == ["action_list_operator"]


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
            os.path.dirname(checkpoints_file),  # noqa: PTH120
            "another.yml",
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
    with pytest.raises(gx_exceptions.DataContextError):
        context.get_legacy_checkpoint("not_a_checkpoint")


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

    with pytest.raises(gx_exceptions.InvalidBaseYamlConfigError):
        context.get_legacy_checkpoint("my_checkpoint")


@pytest.mark.unit
def test_get_checkpoint(empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    obs = context.get_legacy_checkpoint("my_checkpoint")
    assert isinstance(obs, Checkpoint)
    config = obs.get_config(mode=ConfigOutputModes.JSON_DICT)
    assert isinstance(config, dict)
    assert sorted(config.keys()) == [
        "action_list",
        "batch_request",
        "name",
        "runtime_configuration",
        "suite_parameters",
        "validations",
    ]


@pytest.mark.big
def test_run_checkpoint_new_style(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    # add Checkpoint config
    checkpoint_config = CheckpointConfig(
        name="my_checkpoint",
        expectation_suite_name="my_expectation_suite",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
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
    checkpoint_config_key = ConfigurationIdentifier(configuration_key=checkpoint_config.name)
    context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)

    checkpoint = context.get_legacy_checkpoint(checkpoint_config.name)
    with pytest.raises(gx_exceptions.DataContextError, match=r"expectation_suite .* not found"):
        checkpoint.run()

    assert len(context.validations_store.list_keys()) == 0

    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    result: CheckpointResult = checkpoint.run()
    assert len(result.list_validation_results()) == 1
    assert result.success

    result: CheckpointResult = checkpoint.run()
    assert len(result.list_validation_results()) == 1
    assert len(context.validations_store.list_keys()) == 2
    assert result.success


@pytest.mark.filesystem
def test_get_validator_with_instantiated_expectation_suite(
    empty_data_context_stats_enabled, tmp_path_factory
):
    context = empty_data_context_stats_enabled

    base_directory = str(
        tmp_path_factory.mktemp("test_get_validator_with_instantiated_expectation_suite")
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
        expectation_suite=ExpectationSuite("my_expectation_suite"),
    )
    assert my_validator.expectation_suite_name == "my_expectation_suite"


@pytest.mark.filesystem
def test_get_validator_with_attach_expectation_suite(empty_data_context, tmp_path_factory):
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
def test_add_expectation_to_expectation_suite(empty_data_context_stats_enabled):
    context = empty_data_context_stats_enabled

    expectation_suite: ExpectationSuite = context.add_expectation_suite(
        expectation_suite_name="my_new_expectation_suite"
    )
    expectation_suite.add_expectation_configuration(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
        )
    )


@pytest.mark.filesystem
def test_stores_suite_parameters_resolve_correctly(data_context_with_query_store):
    """End to end test demonstrating usage of Stores suite parameters"""
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
            "$PARAMETER": "abs(-urn:great_expectations:stores:my_query_store:col_count - urn:great_expectations:stores:my_query_store:dist_col_count) + 4"  # noqa: E501
        }
    )

    checkpoint_config = {
        "validations": [{"batch_request": batch_request, "expectation_suite_name": suite_name}],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    }
    checkpoint = Checkpoint(f"_tmp_checkpoint_{suite_name}", context, **checkpoint_config)
    checkpoint_result = checkpoint.run()
    assert checkpoint_result.get("success") is True


@pytest.mark.filesystem
def test_modifications_to_env_vars_is_recognized_within_same_program_execution(
    empty_data_context, monkeypatch
) -> None:
    """
    What does this test do and why?

    Great Expectations recognizes changes made to environment variables within a program execution
    and ensures that these changes are recognized in subsequent calls within the same process.

    This is particularly relevant when performing substitutions within a user's project config.
    """
    context = empty_data_context
    env_var_name: str = "MY_PLUGINS_DIRECTORY"
    env_var_value: str = "my_patched_value"

    context.variables.config.plugins_directory = f"${env_var_name}"
    monkeypatch.setenv(env_var_name, env_var_value)

    assert context.plugins_directory and context.plugins_directory.endswith(env_var_value)


@pytest.mark.filesystem
def test_modifications_to_config_vars_is_recognized_within_same_program_execution(
    empty_data_context,
) -> None:
    """
    What does this test do and why?

    Great Expectations recognizes changes made to config variables within a program execution
    and ensures that these changes are recognized in subsequent calls within the same process.

    This is particularly relevant when performing substitutions within a user's project config.
    """
    context = empty_data_context
    config_var_name: str = "my_plugins_dir"
    config_var_value: str = "my_patched_value"

    context.variables.config.plugins_directory = f"${config_var_name}"
    context.save_config_variable(config_variable_name=config_var_name, value=config_var_value)

    assert context.plugins_directory and context.plugins_directory.endswith(config_var_value)


@pytest.mark.big
def test_check_for_usage_stats_sync_finds_diff(
    empty_data_context_stats_enabled,
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
def test_check_for_usage_stats_sync_does_not_find_diff(
    empty_data_context_stats_enabled,
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
def test_check_for_usage_stats_sync_short_circuits_due_to_disabled_usage_stats(
    empty_data_context,
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
    @renderer(renderer_type=".".join([AtomicRendererType.PRESCRIPTIVE, "custom_renderer_type"]))
    def _prescriptive_renderer_custom(
        cls,
        **kwargs: dict,
    ) -> None:
        raise ValueError("This renderer is broken!")

    @override
    def _validate(  # type: ignore[override]
        self,
        **kwargs: dict,
    ) -> Dict[str, Union[bool, dict]]:
        return {
            "success": True,
            "result": {"observed_value": "blue"},
        }


@pytest.mark.xfail(
    reason="Uses unsupported expectation but tests required behavior - fix this test as part of V1-117"  # noqa: E501
)
@pytest.mark.filesystem
def test_unrendered_and_failed_prescriptive_renderer_behavior(
    empty_data_context,
):
    context = empty_data_context

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
    expectation_suite = context.suites.get(name=expectation_suite_name)
    assert not any(
        expectation_configuration.rendered_content
        for expectation_configuration in expectation_suite.expectation_configurations
    )

    # Once we include_rendered_content, we get rendered_content on each ExpectationConfiguration in the ExpectationSuite.  # noqa: E501
    context.variables.include_rendered_content.expectation_suite = True
    expectation_suite = context.suites.get(name=expectation_suite_name)
    for expectation_configuration in expectation_suite.expectation_configurations:
        assert all(
            isinstance(rendered_content_block, RenderedAtomicContent)
            for rendered_content_block in expectation_configuration.rendered_content
        )

    # If we change the ExpectationSuite to use an Expectation that has two content block renderers, one of which is  # noqa: E501
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
    expectation_suite = context.suites.get(name=expectation_suite_name)

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
            exception='Renderer "atomic.prescriptive.custom_renderer_type" failed to render Expectation '  # noqa: E501
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
    for expectation_configuration in expectation_suite.expectation_configurations:
        actual_rendered_content.extend(expectation_configuration.rendered_content)

    assert actual_rendered_content == expected_rendered_content

    # If we have a legacy ExpectationSuite with successful rendered_content blocks, but the new renderer is broken,  # noqa: E501
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

    expectation_suite.expectation_configurations[0].rendered_content = legacy_rendered_content

    actual_rendered_content: List[RenderedAtomicContent] = []
    for expectation_configuration in expectation_suite.expectation_configurations:
        actual_rendered_content.extend(expectation_configuration.rendered_content)

    assert actual_rendered_content == legacy_rendered_content


@pytest.mark.filesystem
def test_file_backed_context_scaffolds_gitignore(tmp_path: pathlib.Path):
    project_path = tmp_path / "my_project_root_dir"
    context_path = project_path / FileDataContext.GX_DIR
    uncommitted = context_path / FileDataContext.GX_UNCOMMITTED_DIR
    gitignore = context_path / FileDataContext.GITIGNORE

    assert not uncommitted.exists()
    assert not gitignore.exists()

    # Scaffold project directory
    _ = get_context(context_root_dir=context_path)

    assert uncommitted.exists()
    assert gitignore.exists()
    assert FileDataContext.GX_UNCOMMITTED_DIR in gitignore.read_text()


@pytest.mark.filesystem
def test_file_backed_context_updates_existing_gitignore(tmp_path: pathlib.Path):
    project_path = tmp_path / "my_project_root_dir"
    context_path = project_path / FileDataContext.GX_DIR
    uncommitted = context_path / FileDataContext.GX_UNCOMMITTED_DIR
    gitignore = context_path / FileDataContext.GITIGNORE

    # Scaffold necessary files so `get_context` updates rather than creates
    uncommitted.mkdir(parents=True)
    existing_value = "__pycache__/"
    with gitignore.open("w") as f:
        f.write(f"\n{existing_value}")

    # Scaffold project directory
    _ = get_context(context_root_dir=context_path)

    contents = gitignore.read_text()
    assert existing_value in contents
    assert FileDataContext.GX_UNCOMMITTED_DIR in contents


@pytest.mark.unit
def test_set_oss_id_with_empty_config(in_memory_runtime_context: EphemeralDataContext):
    context = in_memory_runtime_context
    config = configparser.ConfigParser()

    oss_id = context._set_oss_id(config)

    assert config.sections() == ["anonymous_usage_statistics"]
    assert list(config["anonymous_usage_statistics"]) == ["oss_id"]
    assert oss_id == uuid.UUID(config["anonymous_usage_statistics"]["oss_id"])


@pytest.mark.unit
def test_set_oss_id_with_existing_config(
    in_memory_runtime_context: EphemeralDataContext,
):
    context = in_memory_runtime_context

    # Set up existing config
    # [anonymous_usage_statistics]
    # usage_statistics_url=https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics
    config = configparser.ConfigParser()
    config["anonymous_usage_statistics"] = {}
    usage_statistics_url = (
        "https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics"
    )
    config["anonymous_usage_statistics"]["usage_statistics_url"] = usage_statistics_url

    oss_id = context._set_oss_id(config)

    assert config.sections() == ["anonymous_usage_statistics"]
    assert list(config["anonymous_usage_statistics"]) == [
        "usage_statistics_url",
        "oss_id",
    ]
    assert usage_statistics_url == config["anonymous_usage_statistics"]["usage_statistics_url"]
    assert oss_id == uuid.UUID(config["anonymous_usage_statistics"]["oss_id"])
