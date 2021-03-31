import logging
import os
import shutil
import subprocess
import unittest
from typing import List
from unittest import mock

import nbformat
import pandas as pd
import pytest
from click.testing import CliRunner, Result
from nbconvert.preprocessors import ExecutePreprocessor
from nbformat import NotebookNode
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource import Datasource
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

logger = logging.getLogger(__name__)


@pytest.fixture
def titanic_data_context_with_sql_datasource(
    sa,
    titanic_data_context_stats_enabled_config_version_3,
    test_df,
):
    context: DataContext = titanic_data_context_stats_enabled_config_version_3

    db_file_path: str = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "titanic_sql_test_cases.db"),
    )
    sqlite_engine: sa.engine.base.Engine = sa.create_engine(f"sqlite:///{db_file_path}")
    # noinspection PyUnusedLocal
    conn: sa.engine.base.Connection = sqlite_engine.connect()
    try:
        csv_path: str = file_relative_path(
            __file__, os.path.join("..", "test_sets", "Titanic.csv")
        )
        df: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)
        df.to_sql(name="titanic", con=sqlite_engine)
        df = df.sample(frac=0.5, replace=True, random_state=1)
        df.to_sql(name="incomplete", con=sqlite_engine)
        test_df.to_sql(name="wrong", con=sqlite_engine)
    except ValueError as ve:
        logger.warning(f"Unable to store information into database: {str(ve)}")

    config = yaml.load(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file_path}
"""
        + """
introspection:
    whole_table: {}
""",
    )

    try:
        # noinspection PyUnusedLocal
        my_sql_datasource = context.add_datasource(
            "test_sqlite_db_datasource", **config
        )
    except AttributeError:
        pytest.skip("SQL Database tests require sqlalchemy to be installed.")

    return context


@pytest.fixture
def titanic_data_context_with_spark_datasource(
    tmp_path_factory,
    spark_session,
    test_df,
    monkeypatch,
):
    # Reenable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_fixtures",
                "great_expectations_v013_no_datasource_stats_enabled.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(
            os.path.join(
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )

    context: DataContext = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: SparkDFExecutionEngine

        data_connectors:
            my_basic_data_connector:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {data_path}
                default_regex:
                    pattern: (.*)\\.csv
                    group_names:
                        - data_asset_name

            my_special_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users:
                        base_directory: {data_path}
                        pattern: (.+)_(\\d+)_(\\d+)\\.csv
                        group_names:
                            - name
                            - timestamp
                            - size

            my_other_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users: {{}}
        """

    # noinspection PyUnusedLocal
    datasource: Datasource = context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )
    # noinspection PyProtectedMember
    context._save_project_config()

    csv_path: str

    # To fail an expectation, make number of rows less than 1313 (the original number of rows in the "Titanic" dataset).
    csv_path = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_1911.csv"
    )
    df: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)
    df = df.sample(frac=0.5, replace=True, random_state=1)
    df.to_csv(path_or_buf=csv_path)

    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    # mangle the csv
    with open(csv_path, "w") as f:
        f.write("foo,bar\n1,2\n")
    return context


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_delete_with_non_existent_checkpoint(
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    context: DataContext = empty_data_context_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint delete my_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Could not find Checkpoint `my_checkpoint` (or its configuration is invalid)."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_delete_with_single_checkpoint_confirm_success(
    mock_emit,
    caplog,
    monkeypatch,
    empty_context_with_checkpoint_v1_stats_enabled,
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint delete my_v1_checkpoint",
        input="\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert 'Checkpoint "my_v1_checkpoint" deleted.' in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )

    result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout = result.stdout
    assert "No Checkpoints found." in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_delete_with_single_checkpoint_assume_yes_flag(
    mock_emit,
    caplog,
    monkeypatch,
    empty_context_with_checkpoint_v1_stats_enabled,
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    runner: CliRunner = CliRunner(mix_stderr=False)
    checkpoint_name: str = "my_v1_checkpoint"
    result: Result = runner.invoke(
        cli,
        f"--v3-api --assume-yes checkpoint delete {checkpoint_name}",
        catch_exceptions=False,
    )
    stdout: str = result.stdout
    assert result.exit_code == 0

    assert (
        f'Are you sure you want to delete the Checkpoint "{checkpoint_name}" (this action is irreversible)?'
        not in stdout
    )
    # This assertion is extra assurance since this test is too permissive if we change the confirmation message
    assert "[Y/n]" not in stdout

    assert 'Checkpoint "my_v1_checkpoint" deleted.' in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )

    result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "No Checkpoints found." in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_delete_with_single_checkpoint_cancel_success(
    mock_emit,
    caplog,
    monkeypatch,
    empty_context_with_checkpoint_v1_stats_enabled,
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint delete my_v1_checkpoint",
        input="n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert 'The Checkpoint "my_v1_checkpoint" was not deleted.  Exiting now.' in stdout

    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )

    result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout = result.stdout
    assert "Found 1 Checkpoint." in stdout
    assert "my_v1_checkpoint" in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_list_with_no_checkpoints(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "No Checkpoints found." in stdout
    assert "Use the command `great_expectations checkpoint new` to create one" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.list",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_list_with_single_checkpoint(
    mock_emit,
    caplog,
    monkeypatch,
    empty_context_with_checkpoint_v1_stats_enabled,
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Found 1 Checkpoint." in stdout
    assert "my_v1_checkpoint" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.list",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_list_with_eight_checkpoints(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Found 8 Checkpoints." in stdout
    checkpoint_names_list: List[str] = [
        "my_simple_checkpoint_with_slack_and_notify_with_all",
        "my_nested_checkpoint_template_1",
        "my_nested_checkpoint_template_3",
        "my_nested_checkpoint_template_2",
        "my_simple_checkpoint_with_site_names",
        "my_minimal_simple_checkpoint",
        "my_simple_checkpoint_with_slack",
        "my_simple_template_checkpoint",
    ]
    assert all([checkpoint_name in stdout for checkpoint_name in checkpoint_names_list])

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.list",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_raises_error_on_existing_checkpoint(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    """
    What does this test and why?
    The `checkpoint new` CLI flow should raise an error if the checkpoint name being created already exists in your checkpoint store.
    """
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint new my_minimal_simple_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "A Checkpoint named `my_minimal_simple_checkpoint` already exists. Please choose a new name."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.new",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        caplog,
        result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_checkpoint_new_happy_path_generates_a_notebook_and_checkpoint(
    mock_webbroser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    deterministic_asset_dataconnector_context,
    titanic_expectation_suite,
):
    """
    What does this test and why?
    The v3 (Batch Request) API `checkpoint new` CLI flow includes creating a notebook to configure the checkpoint.
    This test builds that notebook and runs it to generate a checkpoint and then tests the resulting configuration in the checkpoint file.
    The notebook that is generated does create a sample configuration using one of the available Data Assets, this is what is used to generate the checkpoint configuration.
    """
    context: DataContext = deterministic_asset_dataconnector_context

    root_dir: str = context.root_directory
    monkeypatch.chdir(os.path.dirname(root_dir))

    assert context.list_checkpoints() == []
    context.save_expectation_suite(titanic_expectation_suite)
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    # Clear the "data_context.save_expectation_suite" call
    mock_emit.reset_mock()

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint new passengers",
        input="1\n1\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "open a notebook for you now" in stdout

    assert mock_emit.call_count == 2

    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.new",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    assert mock_subprocess.call_count == 1
    assert mock_webbroser.call_count == 0

    expected_notebook_path: str = os.path.join(
        root_dir, "uncommitted", "edit_checkpoint_passengers.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    with open(expected_notebook_path) as f:
        nb: NotebookNode = nbformat.read(f, as_version=4)

    uncommitted_dir: str = os.path.join(root_dir, "uncommitted")
    # Run notebook
    # TODO: <ANTHONY>We should mock the datadocs call or skip running that cell within the notebook (rather than commenting it out in the notebook)</ANTHONY>
    ep: ExecutePreprocessor = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    # Ensure the checkpoint file was created
    expected_checkpoint_path: str = os.path.join(
        root_dir, "checkpoints", "passengers.yml"
    )
    assert os.path.isfile(expected_checkpoint_path)

    # Ensure the checkpoint configuration in the file is as expected
    with open(expected_checkpoint_path) as f:
        checkpoint_config: str = f.read()
    expected_checkpoint_config: str = """name: passengers
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request:
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
      datasource_name: my_datasource
      data_connector_name: my_other_data_connector
      data_asset_name: users
      data_connector_query:
        index: -1
    expectation_suite_name: Titanic.warning
profilers: []
"""
    assert checkpoint_config == expected_checkpoint_config

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_raises_error_if_checkpoint_is_not_found(
    mock_emit, caplog, monkeypatch, empty_context_with_checkpoint_v1_stats_enabled
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Could not find Checkpoint `my_checkpoint` (or its configuration is invalid)."
        in stdout
    )
    assert "Try running" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_not_found_suite_raises_error(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_stats_enabled_with_checkpoints_v1_with_templates

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_nested_checkpoint_template_1",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "expectation_suite suite_from_template_1 not found" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_batch_load_problem_raises_error(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="bar"
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert context.list_expectation_suite_names() == ["bar"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "bad_batch.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: bad_batch
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
          batch_spec_passthrough:
            path: /totally/not/a/file.csv
            reader_method: read_csv
        expectation_suite_name: bar
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    monkeypatch.chdir(os.path.dirname(context.root_directory))
    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run bad_batch",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout

    # TODO: <Alex>ALEX -- Investigate how to make Abe's suggestion a reality.</Alex>
    # Note: Abe : 2020/09: This was a better error message, but it should live in DataContext.get_batch, not a random CLI method.
    # assert "There was a problem loading a batch:" in stdout
    # assert (
    #     "{'path': '/totally/not/a/file.csv', 'datasource': 'mydatasource', 'reader_method': 'read_csv'}"
    #     in stdout
    # )
    # assert (
    #     "Please verify these batch kwargs in checkpoint bad_batch`"
    #     in stdout
    # )
    # assert "No such file or directory" in stdout
    assert ("No such file or directory" in stdout) or ("does not exist" in stdout)

    assert mock_emit.call_count == 3

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "f6e1151b49fceb15ae3de4eb60f62be4",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert actual_events == expected_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_empty_suite_list_raises_error(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert context.list_expectation_suite_names() == []

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "no_suite.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run no_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Exception occurred while running Checkpoint" in stdout
    assert (
        "of Checkpoint 'no_suite': validation expectation_suite_name must be specified"
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_non_existent_validations(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert context.list_expectation_suite_names() == []

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "no_validations.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_base_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
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
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
        result_format:
          result_format: BASIC
          partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run no_validations",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert 'Checkpoint "no_validations" does not contain any validations.' in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_successful_validation_pandas(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert all(
        [
            msg in stdout
            for msg in [
                "Validation succeeded!",
                "Titanic.warning",
                "Passed",
                "100.0 %",
            ]
        ]
    )

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.build_data_docs",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_successful_validation_sql(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_sql_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_sql_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: test_sqlite_db_datasource
          data_connector_name: whole_table
          data_asset_name: titanic
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert all(
        [
            msg in stdout
            for msg in [
                "Validation succeeded!",
                "Titanic.warning",
                "Passed",
                "100.0 %",
            ]
        ]
    )

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_successful_validation_spark(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_spark_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_spark_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_basic_data_connector
          batch_spec_passthrough:
            reader_options:
              header: true
          data_asset_name: Titanic_1912
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert all(
        [
            msg in stdout
            for msg in [
                "Validation succeeded!",
                "Titanic.warning",
                "Passed",
                "100.0 %",
            ]
        ]
    )

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.build_data_docs",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_pandas(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    # To fail an expectation, make number of rows less than 1313 (the original number of rows in the "Titanic" dataset).
    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    df: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)
    df = df.sample(frac=0.5, replace=True, random_state=1)
    df.to_csv(path_or_buf=csv_path)

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Validation failed!" in stdout

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.build_data_docs",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_sql(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_sql_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_sql_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: test_sqlite_db_datasource
          data_connector_name: whole_table
          data_asset_name: incomplete
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Validation failed!" in stdout

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_spark(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_spark_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_spark_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_basic_data_connector
          data_asset_name: Titanic_1911
          data_connector_query:
            index: -1
          batch_spec_passthrough:
            reader_options:
              header: true
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Validation failed!" in stdout

    assert mock_emit.call_count == 5

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.__init__",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_due_to_bad_data_pandas(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    # mangle the csv
    with open(csv_path, "w") as f:
        f.write("foo,bar\n1,2\n")

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Exception occurred while running Checkpoint." in stdout
    assert 'Error: The column "Name" in BatchData does not exist...' in stdout

    assert mock_emit.call_count == 4

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": False,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_due_to_bad_data_sql(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_sql_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_sql_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: test_sqlite_db_datasource
          data_connector_name: whole_table
          data_asset_name: wrong
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Exception occurred while running Checkpoint." in stdout
    assert 'Error: The column "Name" in BatchData does not exist...' in stdout

    assert mock_emit.call_count == 4

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": False,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_happy_path_with_failed_validation_due_to_bad_data_spark(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_with_spark_datasource,
    titanic_expectation_suite,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_data_context_with_spark_datasource
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    # mangle the csv
    with open(csv_path, "w") as f:
        f.write("foo,bar\n1,2\n")

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
          batch_spec_passthrough:
            reader_options:
              header: true
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint run my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Exception occurred while running Checkpoint." in stdout
    assert 'Error: The column "Name" in BatchData does not exist...' in stdout

    assert mock_emit.call_count == 4

    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event_payload": {
                    "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                },
                "event": "data_context.save_expectation_suite",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_asset.validate",
                "event_payload": {
                    "anonymized_batch_kwarg_keys": [],
                    "anonymized_expectation_suite_name": "__not_found__",
                    "anonymized_datasource_name": "__not_found__",
                },
                "success": False,
            }
        ),
        mock.call(
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert expected_events == actual_events

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_script_raises_error_if_checkpoint_not_found(
    mock_emit, caplog, monkeypatch, empty_context_with_checkpoint_v1_stats_enabled
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled
    assert context.list_checkpoints() == ["my_v1_checkpoint"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script not_a_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Could not find Checkpoint `not_a_checkpoint` (or its configuration is invalid)."
        in stdout
    )
    assert "Try running" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.script",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_script_raises_error_if_python_file_exists(
    mock_emit, caplog, monkeypatch, empty_context_with_checkpoint_v1_stats_enabled
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    assert context.list_checkpoints() == ["my_v1_checkpoint"]

    script_path: str = os.path.join(
        context.root_directory, context.GE_UNCOMMITTED_DIR, "run_my_v1_checkpoint.py"
    )
    with open(script_path, "w") as f:
        f.write("script here")
    assert os.path.isfile(script_path)

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script my_v1_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Warning! A script named run_my_v1_checkpoint.py already exists and this command will not overwrite it."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.script",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    # assert the script has original contents
    with open(script_path) as f:
        assert f.read() == "script here"

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_script_happy_path_generates_script_pandas(
    mock_emit, caplog, monkeypatch, empty_context_with_checkpoint_v1_stats_enabled
):
    context: DataContext = empty_context_with_checkpoint_v1_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script my_v1_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert (
        "A python script was created that runs the Checkpoint named: `my_v1_checkpoint`"
        in stdout
    )
    assert (
        "The script is located in `great_expectations/uncommitted/run_my_v1_checkpoint.py`"
        in stdout
    )
    assert (
        "The script can be run with `python great_expectations/uncommitted/run_my_v1_checkpoint.py`"
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.script",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    expected_script: str = os.path.join(
        context.root_directory, context.GE_UNCOMMITTED_DIR, "run_my_v1_checkpoint.py"
    )
    assert os.path.isfile(expected_script)

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_checkpoint_script_happy_path_executable_successful_validation_pandas(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    We call the "checkpoint script" command on a project with a checkpoint.

    The command should:
    - create the script (note output is tested in other tests)

    When run the script should:
    - execute
    - return a 0 status code
    - print a success message
    """
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="users.delivery"
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert context.list_expectation_suite_names() == ["users.delivery"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: users.delivery
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )

    script_path: str = os.path.abspath(
        os.path.join(
            context.root_directory,
            context.GE_UNCOMMITTED_DIR,
            "run_my_fancy_checkpoint.py",
        )
    )
    assert os.path.isfile(script_path)

    # In travis on osx, python may not execute from the build dir
    cmdstring: str = f"python {script_path}"
    if os.environ.get("TRAVIS_OS_NAME") == "osx":
        build_dir: str = os.environ.get("TRAVIS_BUILD_DIR")
        print(os.listdir(build_dir))
        cmdstring = f"python3 {script_path}"
    print("about to run: " + cmdstring)
    print(os.curdir)
    print(os.listdir(os.curdir))
    print(os.listdir(os.path.abspath(os.path.join(context.root_directory, ".."))))

    status: int
    output: str
    status, output = subprocess.getstatusoutput(cmdstring)
    print(f"\n\nScript exited with code: {status} and output:\n{output}")

    assert status == 0
    assert "Validation succeeded!" in output


def test_checkpoint_script_happy_path_executable_failed_validation_pandas(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    titanic_expectation_suite,
):
    """
    We call the "checkpoint script" command on a project with a checkpoint.

    The command should:
    - create the script (note output is tested in other tests)

    When run the script should:
    - execute
    - return a 1 status code
    - print a failure message
    """
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    # To fail an expectation, make number of rows less than 1313 (the original number of rows in the "Titanic" dataset).
    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    df: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)
    df = df.sample(frac=0.5, replace=True, random_state=1)
    df.to_csv(path_or_buf=csv_path)

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )

    script_path: str = os.path.abspath(
        os.path.join(
            context.root_directory,
            context.GE_UNCOMMITTED_DIR,
            "run_my_fancy_checkpoint.py",
        )
    )
    assert os.path.isfile(script_path)

    # In travis on osx, python may not execute from the build dir
    cmdstring: str = f"python {script_path}"
    if os.environ.get("TRAVIS_OS_NAME") == "osx":
        build_dir: str = os.environ.get("TRAVIS_BUILD_DIR")
        print(os.listdir(build_dir))
        cmdstring = f"python3 {script_path}"
    print("about to run: " + cmdstring)
    print(os.curdir)
    print(os.listdir(os.curdir))
    print(os.listdir(os.path.abspath(os.path.join(context.root_directory, ".."))))

    status: int
    output: str
    status, output = subprocess.getstatusoutput(cmdstring)
    print(f"\n\nScript exited with code: {status} and output:\n{output}")
    assert status == 1
    assert "Validation failed!" in output


def test_checkpoint_script_happy_path_executable_failed_validation_due_to_bad_data_pandas(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    titanic_expectation_suite,
):
    """
    We call the "checkpoint script" command on a project with a checkpoint.

    The command should:
    - create the script (note output is tested in other tests)

    When run the script should:
    - execute
    - return a 1 status code
    - print a failure message
    """
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    context.save_expectation_suite(
        expectation_suite=titanic_expectation_suite,
        expectation_suite_name="Titanic.warning",
    )
    assert context.list_expectation_suite_names() == ["Titanic.warning"]

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    csv_path: str = os.path.join(
        context.root_directory, "..", "data", "titanic", "Titanic_19120414_1313.csv"
    )
    # mangle the csv
    with open(csv_path, "w") as f:
        f.write("foo,bar\n1,2\n")

    checkpoint_file_path: str = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        "my_fancy_checkpoint.yml",
    )

    checkpoint_yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          data_connector_query:
            index: -1
        expectation_suite_name: Titanic.warning
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
        evaluation_parameters:
          param1: "$MY_PARAM"
          param2: 1 + "$OLD_PARAM"
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """
    config: dict = dict(yaml.load(checkpoint_yaml_config))
    _write_checkpoint_dict_to_file(
        config=config, checkpoint_file_path=checkpoint_file_path
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint script my_fancy_checkpoint",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )

    script_path: str = os.path.abspath(
        os.path.join(
            context.root_directory,
            context.GE_UNCOMMITTED_DIR,
            "run_my_fancy_checkpoint.py",
        )
    )
    assert os.path.isfile(script_path)

    # In travis on osx, python may not execute from the build dir
    cmdstring: str = f"python {script_path}"
    if os.environ.get("TRAVIS_OS_NAME") == "osx":
        build_dir: str = os.environ.get("TRAVIS_BUILD_DIR")
        print(os.listdir(build_dir))
        cmdstring = f"python3 {script_path}"
    print("about to run: " + cmdstring)
    print(os.curdir)
    print(os.listdir(os.curdir))
    print(os.listdir(os.path.abspath(os.path.join(context.root_directory, ".."))))

    status: int
    output: str
    status, output = subprocess.getstatusoutput(cmdstring)
    print(f"\n\nScript exited with code: {status} and output:\n{output}")
    assert status == 1
    assert (
        'ExecutionEngineError: Error: The column "Name" in BatchData does not exist.'
        in output
    )


@pytest.mark.xfail(
    reason="TODO: ALEX <Alex>NOT_IMPLEMENTED_YET</Alex>",
    run=True,
    strict=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_with_ge_config_3_raises_error(
    mock_emit, caplog, monkeypatch, titanic_data_context_stats_enabled_config_version_3
):
    context: DataContext = titanic_data_context_stats_enabled_config_version_3

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api checkpoint new foo not_a_suite",
        catch_exceptions=False,
    )

    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "The `checkpoint new` CLI command is not yet implemented for Great Expectations config versions >= 3."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.checkpoint.new",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def _write_checkpoint_dict_to_file(config, checkpoint_file_path):
    yaml_obj: YAML = YAML()
    with open(checkpoint_file_path, "w") as f:
        yaml_obj.dump(config, f)
