import os
import shutil

import mock
from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_list(
    mock_emit, caplog, empty_context_with_checkpoint_stats_enabled
):
    context = empty_context_with_checkpoint_stats_enabled
    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint list -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "Found 1 checkpoint." in stdout
    assert "my_checkpoint" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.list", "event_payload": {}, "success": True}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_raises_error_on_existing_checkpoint(
    mock_emit, caplog, empty_context_with_checkpoint_stats_enabled
):
    context = empty_context_with_checkpoint_stats_enabled
    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"checkpoint new my_checkpoint suite -d {root_dir}",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1
    assert (
        "A checkpoint named `my_checkpoint` already exists. Please choose a new name."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.new", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_raises_error_on_no_suite_found(
    mock_emit, caplog, titanic_data_context_stats_enabled
):
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    assert context.list_expectation_suite_names() == []
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint new foo not_a_suite -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1
    assert "Could not find a suite named `not_a_suite`" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.new", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new(
    mock_emit, caplog, titanic_data_context_stats_enabled, titanic_expectation_suite
):
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    assert context.list_checkpoints() == []
    context.save_expectation_suite(titanic_expectation_suite)
    assert context.list_expectation_suite_names() == ["Titanic.warning"]
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"checkpoint new passengers Titanic.warning -d {root_dir}",
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    print(stdout)
    assert result.exit_code == 0
    assert "A checkpoint named `passengers` was added to your project" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.new", "event_payload": {}, "success": True}
        ),
    ]
    expected_checkpoint = os.path.join(
        root_dir, context.CHECKPOINTS_DIR, "passengers.yml"
    )
    assert os.path.isfile(expected_checkpoint)

    # Newup a context for additional assertions
    context = DataContext(root_dir)
    assert context.list_checkpoints() == ["passengers"]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_specify_datasource(
    mock_emit, caplog, titanic_data_context_stats_enabled, titanic_expectation_suite
):
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    assert context.list_checkpoints() == []
    context.save_expectation_suite(titanic_expectation_suite)
    assert context.list_expectation_suite_names() == ["Titanic.warning"]
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"checkpoint new passengers Titanic.warning -d {root_dir} --datasource mydatasource",
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "A checkpoint named `passengers` was added to your project" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.new", "event_payload": {}, "success": True}
        ),
    ]
    expected_checkpoint = os.path.join(
        root_dir, context.CHECKPOINTS_DIR, "passengers.yml"
    )
    assert os.path.isfile(expected_checkpoint)

    # Newup a context for additional assertions
    context = DataContext(root_dir)
    assert context.list_checkpoints() == ["passengers"]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_new_works_if_checkpoints_directory_is_missing(
    mock_emit, caplog, titanic_data_context_stats_enabled, titanic_expectation_suite
):
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    checkpoints_dir = os.path.join(root_dir, context.CHECKPOINTS_DIR)
    shutil.rmtree(checkpoints_dir)
    assert not os.path.isdir(checkpoints_dir)
    assert context.list_checkpoints() == []

    context.save_expectation_suite(titanic_expectation_suite)
    assert context.list_expectation_suite_names() == ["Titanic.warning"]
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"checkpoint new passengers Titanic.warning -d {root_dir}",
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "A checkpoint named `passengers` was added to your project" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.new", "event_payload": {}, "success": True}
        ),
    ]
    expected_checkpoint = os.path.join(
        root_dir, context.CHECKPOINTS_DIR, "passengers.yml"
    )
    assert os.path.isfile(expected_checkpoint)

    # Newup a context for additional assertions
    context = DataContext(root_dir)
    assert context.list_checkpoints() == ["passengers"]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_raises_error_if_checkpoint_is_not_found(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint run fake_checkpoint -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout

    assert "Could not find checkpoint `fake_checkpoint`." in stdout
    assert "Try running" in stdout
    assert result.exit_code == 1

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.run", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_not_found_suite(
    mock_emit, caplog, empty_context_with_checkpoint_stats_enabled
):
    context = empty_context_with_checkpoint_stats_enabled
    root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint run my_checkpoint -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1

    assert "Could not find a suite named `suite_one`" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.run", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_batch_load_problem(
    mock_emit, caplog, titanic_data_context_stats_enabled
):
    context = titanic_data_context_stats_enabled
    suite = context.create_expectation_suite("bar")
    context.save_expectation_suite(suite)
    assert context.list_expectation_suite_names() == ["bar"]
    mock_emit.reset_mock()

    root_dir = context.root_directory
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "bad_batch.yml"
    )
    bad = {
        "batches": [
            {
                "batch_kwargs": {
                    "path": "/totally/not/a/file.csv",
                    "datasource": "mydatasource",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": ["bar"],
            },
        ],
    }
    _write_checkpoint_dict_to_file(bad, checkpoint_file_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint run bad_batch -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1

    assert "There was a problem loading a batch:" in stdout
    assert (
        "{'path': '/totally/not/a/file.csv', 'datasource': 'mydatasource', 'reader_method': 'read_csv'}"
        in stdout
    )
    assert (
        "Please verify these batch kwargs in the checkpoint file: `great_expectations/checkpoints/bad_batch.yml`"
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.run", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_checkpoint_with_empty_suite_list(
    mock_emit, caplog, titanic_data_context_stats_enabled
):
    context = titanic_data_context_stats_enabled
    assert context.list_expectation_suite_names() == []

    root_dir = context.root_directory
    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "bad_batch.yml"
    )
    bad = {
        "batches": [
            {
                "batch_kwargs": {
                    "path": "/totally/not/a/file.csv",
                    "datasource": "mydatasource",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": [],
            },
        ],
    }
    _write_checkpoint_dict_to_file(bad, checkpoint_file_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint run bad_batch -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1

    assert (
        "A batch has no suites associated with it. At least one suite is required."
        in stdout
    )
    assert (
        "Batch: {'path': '/totally/not/a/file.csv', 'datasource': 'mydatasource', 'reader_method': 'read_csv'}"
        in stdout
    )
    assert (
        "Please add at least one suite to your checkpoint file: great_expectations/checkpoints/bad_batch.yml"
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.run", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_run_on_non_existent_validation_operator(
    mock_emit, caplog, titanic_data_context_stats_enabled
):
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    csv_path = os.path.join(root_dir, "..", "data", "Titanic.csv")

    suite = context.create_expectation_suite("iceberg")
    context.save_expectation_suite(suite)
    assert context.list_expectation_suite_names() == ["iceberg"]
    mock_emit.reset_mock()

    checkpoint_file_path = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, "bad_operator.yml"
    )
    bad = {
        "validation_operator_name": "foo",
        "batches": [
            {
                "batch_kwargs": {
                    "path": csv_path,
                    "datasource": "mydatasource",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": ["iceberg"],
            },
        ],
    }
    _write_checkpoint_dict_to_file(bad, checkpoint_file_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"checkpoint run bad_operator -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1

    assert (
        f"No validation operator `foo` was found in your project. Please verify this in your great_expectations.yml"
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.checkpoint.run", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


def _write_checkpoint_dict_to_file(bad, checkpoint_file_path):
    yaml = YAML()
    with open(checkpoint_file_path, "w") as f:
        yaml.dump(bad, f)
