from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli.v012 import cli
from tests.cli.utils import escape_ansi
from tests.cli.v012.utils import assert_no_logging_messages_or_tracebacks


def test_store_list_with_zero_stores(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context._project_config.stores = {}
    context._save_project_config()
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli,
        f"store list -d {project_dir}",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Your configuration file is not a valid yml file likely due to a yml syntax error"
        in result.output.strip()
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_store_list_with_two_stores(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    del context._project_config.stores["validations_store"]
    del context._project_config.stores["evaluation_parameter_store"]
    del context._project_config.stores["profiler_store"]
    context._project_config.validations_store_name = "expectations_store"
    context._project_config.evaluation_parameter_store_name = "expectations_store"
    context._project_config.profiler_store_name = "profiler_store"
    context._save_project_config()

    runner = CliRunner(mix_stderr=False)

    expected_result = """\
2 Stores found:

 - name: expectations_store
   class_name: ExpectationsStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: expectations/

 - name: checkpoint_store
   class_name: CheckpointStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: checkpoints/
     suppress_store_backend_id: True"""

    result = runner.invoke(
        cli,
        f"store list -d {project_dir}",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert escape_ansi(result.output).strip() == expected_result.strip()

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_store_list_with_four_stores(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    expected_result = """\
5 Stores found:

 - name: expectations_store
   class_name: ExpectationsStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: expectations/

 - name: validations_store
   class_name: ValidationsStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: uncommitted/validations/

 - name: evaluation_parameter_store
   class_name: EvaluationParameterStore

 - name: checkpoint_store
   class_name: CheckpointStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: checkpoints/
     suppress_store_backend_id: True

 - name: profiler_store
   class_name: ProfilerStore
   store_backend:
     class_name: TupleFilesystemStoreBackend
     base_directory: profilers/
     suppress_store_backend_id: True"""
    result = runner.invoke(
        cli,
        f"store list -d {project_dir}",
        catch_exceptions=False,
    )
    print(result.output)
    assert result.exit_code == 0
    assert escape_ansi(result.output).strip() == expected_result.strip()

    assert_no_logging_messages_or_tracebacks(caplog, result)
