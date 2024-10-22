from __future__ import annotations

import configparser
import os
import pathlib
import shutil
import uuid
from typing import Dict, List, Union

import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import get_context
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.data_context.cloud_data_context import CloudDataContext
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.util import file_relative_path
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
from tests.test_utils import safe_remove

yaml = YAMLHandler()

parameterized_expectation_suite_name = "my_dag_node.default"


@pytest.fixture
def data_context_with_bad_datasource(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want.

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


@pytest.mark.cloud
def test_get_expectation_suite_include_rendered_content(
    empty_cloud_data_context: CloudDataContext,
):
    context = empty_cloud_data_context

    expectation_suite: ExpectationSuite = context.suites.add(
        ExpectationSuite(name="this_data_asset_config_does_not_exist.default")
    )
    expectation_suite.expectation_configurations.append(
        ExpectationConfiguration(type="expect_table_row_count_to_equal", kwargs={"value": 10})
    )
    for expectation in expectation_suite.expectation_configurations:
        assert expectation.rendered_content is None
    expectation_suite.save()
    context.suites.get("this_data_asset_config_does_not_exist.default")
    for expectation in expectation_suite.expectation_configurations:
        assert expectation.rendered_content is None

    expectation_suite_retrieved: ExpectationSuite = context.suites.get(
        "this_data_asset_config_does_not_exist.default",
    )

    for expectation in expectation_suite_retrieved.expectation_configurations:
        assert expectation.rendered_content
        for rendered_content_block in expectation.rendered_content:
            assert isinstance(
                rendered_content_block,
                RenderedAtomicContent,
            )


@pytest.mark.unit
def test_data_context_get_datasource_on_non_existent_one_raises_helpful_error(
    titanic_data_context: AbstractDataContext,
):
    # this is deprecated
    with pytest.warns(DeprecationWarning), pytest.raises(ValueError):
        _ = titanic_data_context.get_datasource("fakey_mc_fake")


@pytest.mark.unit
def test_data_context_get_datasource(
    titanic_data_context: AbstractDataContext,
):
    # this is deprecated
    name = "my datasource"
    ds = titanic_data_context.data_sources.add_pandas(name)
    with pytest.warns(DeprecationWarning) as warning_records:
        fetched_ds = titanic_data_context.get_datasource(name)

    assert fetched_ds == ds
    assert len(warning_records) == 1
    assert "context.get_datasource is deprecated" in str(warning_records.list[0].message)


@pytest.mark.unit
def test_add_store(empty_data_context):
    assert "my_new_store" not in empty_data_context.stores
    assert "my_new_store" not in empty_data_context.get_config()["stores"]
    new_store = empty_data_context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        },
    )
    assert "my_new_store" in empty_data_context.stores
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
    assert context._normalize_absolute_or_relative_path("/yikes") == "/yikes"


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
def test_data_context_create_does_not_raise_error_or_warning_if_ge_dir_exists(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    gx.get_context(mode="file", project_root_dir=project_path)


@pytest.fixture()
def empty_context(tmp_path_factory) -> FileDataContext:
    project_path = str(tmp_path_factory.mktemp("data_context"))
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    context = get_context(context_root_dir=ge_dir)
    assert isinstance(context, FileDataContext)
    assert os.path.isdir(ge_dir)  # noqa: PTH112
    assert os.path.isfile(  # noqa: PTH113
        os.path.join(ge_dir, FileDataContext.GX_YML)  # noqa: PTH118
    )
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
    empty_context.data_sources.add_pandas("arthur")
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
    context.data_sources.add_pandas("arthur")
    context.suites.add(ExpectationSuite(name="dent"))
    assert len(context.suites.all()) == 1

    assert FileDataContext.is_project_initialized(ge_dir) is True


@pytest.mark.filesystem
def test_data_context_is_project_initialized_returns_true_when_its_valid_context_has_one_datasource_and_no_suites(  # noqa: E501
    empty_context,
):
    context = empty_context
    ge_dir = context.root_directory
    context.data_sources.add_pandas("arthur")
    assert len(context.suites.all()) == 0

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
    gx.get_context(mode="file", project_root_dir=project_path)
    ge_yml = os.path.join(project_path, "gx/great_expectations.yml")  # noqa: PTH118
    with open(ge_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    gx.get_context(mode="file", project_root_dir=project_path)

    with open(ge_yml) as ff:
        obs = ff.read()
    assert "# LOOK I WAS MODIFIED" in obs


@pytest.mark.filesystem
def test_data_context_create_makes_uncommitted_dirs_when_all_are_missing(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("data_context"))
    gx.get_context(mode="file", project_root_dir=project_path)

    # mangle the existing setup
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    shutil.rmtree(uncommitted_dir)

    # re-run create to simulate onboarding
    gx.get_context(mode="file", project_root_dir=project_path)
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
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
            .ge_store_backend_id
    validation_definitions/
"""
    project_path = str(tmp_path_factory.mktemp("stuff"))
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118

    gx.get_context(mode="file", project_root_dir=project_path)
    fixture = gen_directory_tree_str(ge_dir)

    assert fixture == expected

    # re-run create to simulate onboarding
    gx.get_context(mode="file", project_root_dir=project_path)

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
    gx.get_context(mode="file", project_root_dir=project_path)
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
    context = gx.get_context(mode="file", project_root_dir=project_path)
    assert isinstance(context, FileDataContext)

    for directory in [
        "expectations",
        "plugins",
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
    gx.get_context(mode="file", project_root_dir=project_path)
    ge_dir = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")  # noqa: PTH118
    config_vars_yml = os.path.join(  # noqa: PTH118
        uncommitted_dir, "config_variables.yml"
    )

    # modify config variables
    with open(config_vars_yml, "a") as ff:
        ff.write("# LOOK I WAS MODIFIED")

    # re-run create to simulate onboarding
    gx.get_context(mode="file", project_root_dir=project_path)

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
    finally:
        # Make sure we unset the environment variable we're using
        monkeypatch.delenv("TEST_CONFIG_FILE_ENV")


@pytest.mark.filesystem
def test_add_expectation_to_expectation_suite(empty_data_context_stats_enabled):
    context = empty_data_context_stats_enabled

    expectation_suite: ExpectationSuite = context.suites.add(
        ExpectationSuite(name="my_new_expectation_suite")
    )
    expectation_suite.add_expectation_configuration(
        ExpectationConfiguration(type="expect_table_row_count_to_equal", kwargs={"value": 10})
    )


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
    context.save_config_variable(name=config_var_name, value=config_var_value)

    assert context.plugins_directory and context.plugins_directory.endswith(config_var_value)


class ExpectSkyToBeColor(BatchExpectation):
    metric_dependencies = ("table.color",)
    success_keys = ("color",)
    args_keys = ("color",)

    color: str

    @classmethod
    @renderer(renderer_type=".".join([AtomicRendererType.PRESCRIPTIVE, "custom_renderer_type"]))
    def _prescriptive_renderer_custom(
        cls,
        **kwargs: dict,
    ) -> None:
        raise ValueError("This renderer is broken!")

    def _validate(  # type: ignore[override,explicit-override] # FIXME
        self,
        **kwargs: dict,
    ) -> Dict[str, Union[bool, dict]]:
        return {
            "success": True,
            "result": {"observed_value": "blue"},
        }


class TestRenderedContent:
    @pytest.mark.filesystem
    def test_no_rendered_content_for_file_data_context(self, empty_data_context: FileDataContext):
        suite_name = "test_suite"
        empty_data_context.suites.add(
            ExpectationSuite(
                name=suite_name,
                expectations=[gx.expectations.ExpectTableRowCountToEqual(value=0)],
            )
        )
        expectation_suite = empty_data_context.suites.get(name=suite_name)

        assert all(
            expectation.rendered_content is None for expectation in expectation_suite.expectations
        )

    @pytest.mark.cloud
    def test_rendered_content_for_cloud(self, empty_cloud_data_context: CloudDataContext) -> None:
        suite_name = "test_suite"
        empty_cloud_data_context.suites.add(
            ExpectationSuite(
                name=suite_name,
                expectations=[gx.expectations.ExpectTableRowCountToEqual(value=0)],
            )
        )
        expectation_suite = empty_cloud_data_context.suites.get(name=suite_name)

        rendered_content_blocks: list = []
        for expectation in expectation_suite.expectations:
            assert expectation.rendered_content is not None
            rendered_content_blocks.extend(expectation.rendered_content)
        assert rendered_content_blocks

    @pytest.mark.cloud
    def test_multiple_rendered_content_blocks_one_is_busted(
        self, empty_cloud_data_context: CloudDataContext
    ) -> None:
        suite_name = "test_suite"
        empty_cloud_data_context.suites.add(
            ExpectationSuite(
                name=suite_name,
                expectations=[
                    ExpectSkyToBeColor(color="blue"),
                ],
            )
        )
        expectation_suite = empty_cloud_data_context.suites.get(name=suite_name)

        expected_rendered_content: List[RenderedAtomicContent] = [
            RenderedAtomicContent(
                name=AtomicPrescriptiveRendererType.FAILED,
                value=renderedAtomicValueSchema.load(
                    {
                        "template": (
                            "Rendering failed for Expectation: $expectation_type(**$kwargs)."
                        ),
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
        for expectation in expectation_suite.expectations:
            assert expectation.rendered_content is not None
            actual_rendered_content.extend(expectation.rendered_content)

        assert actual_rendered_content == expected_rendered_content


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

    assert config.sections() == ["analytics"]
    assert list(config["analytics"]) == ["oss_id"]
    assert oss_id == uuid.UUID(config["analytics"]["oss_id"])


@pytest.mark.unit
def test_set_oss_id_with_existing_config(
    in_memory_runtime_context: EphemeralDataContext,
):
    context = in_memory_runtime_context

    # Set up existing config
    config = configparser.ConfigParser()
    config["analytics"] = {}

    oss_id = context._set_oss_id(config)

    assert config.sections() == ["analytics"]
    assert list(config["analytics"]) == [
        "oss_id",
    ]
    assert oss_id == uuid.UUID(config["analytics"]["oss_id"])
