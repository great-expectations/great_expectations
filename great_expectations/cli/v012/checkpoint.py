import os
import sys
from typing import Dict

import click
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.cli.v012 import toolkit
from great_expectations.cli.v012.mark import Mark as mark
from great_expectations.cli.v012.util import cli_message, cli_message_list
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.data_context.types.base import DataContextConfigDefaults
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidTopLevelConfigKeyError
from great_expectations.util import lint_code
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    SQLAlchemyError = RuntimeError
try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    SQLAlchemyError = RuntimeError
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
"\n--ge-feature-maturity-info--\n\n    id: checkpoint_command_line\n    title: LegacyCheckpoint - Command Line\n    icon:\n    short_description: Run a configured legacy checkpoint from a command line.\n    description: Run a configured legacy checkpoint from a command line in a Terminal shell.\n    how_to_guide_url: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_run_a_checkpoint_in_terminal.html\n    maturity: Experimental\n    maturity_details:\n        api_stability: Unstable (expect changes to batch request; no checkpoint store)\n        implementation_completeness: Complete\n        unit_test_coverage: Complete\n        integration_infrastructure_test_coverage: N/A\n        documentation_completeness: Complete\n        bug_risk: Low\n\n--ge-feature-maturity-info--\n"


@click.group(short_help="Checkpoint operations")
def checkpoint() -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    Checkpoint operations\n\n    A checkpoint is a bundle of one or more batches of data with one or more\n    Expectation Suites.\n\n    A checkpoint can be as simple as one batch of data paired with one\n    Expectation Suite.\n\n    A checkpoint can be as complex as many batches of data across different\n    datasources paired with one or more Expectation Suites each.\n    "
    pass


@checkpoint.command(name="new")
@click.argument("checkpoint")
@click.argument("suite")
@click.option("--datasource", default=None)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_new(checkpoint, suite, directory, datasource) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Create a new checkpoint for easy deployments. (Experimental)"
    suite_name = suite
    usage_event = "cli.checkpoint.new"
    context = toolkit.load_data_context_with_error_handling(directory)
    _verify_checkpoint_does_not_exist(context, checkpoint, usage_event)
    suite: ExpectationSuite = toolkit.load_expectation_suite(
        context, suite_name, usage_event
    )
    datasource = toolkit.select_datasource(context, datasource_name=datasource)
    if datasource is None:
        send_usage_message(
            data_context=context, event=usage_event, api_version="v2", success=False
        )
        sys.exit(1)
    (_, _, _, batch_kwargs) = toolkit.get_batch_kwargs(context, datasource.name)
    _ = context.add_checkpoint(
        name=checkpoint,
        **{
            "class_name": "LegacyCheckpoint",
            "batches": [
                {
                    "batch_kwargs": dict(batch_kwargs),
                    "expectation_suite_names": [suite.expectation_suite_name],
                }
            ],
        },
    )
    cli_message(
        f"""<green>A Checkpoint named `{checkpoint}` was added to your project!</green>
  - To run this Checkpoint, run `great_expectations --v2-api checkpoint run {checkpoint}`"""
    )
    send_usage_message(
        data_context=context, event=usage_event, api_version="v2", success=True
    )


def _verify_checkpoint_does_not_exist(
    context: DataContext, checkpoint: str, usage_event: str
) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    try:
        if checkpoint in context.list_checkpoints():
            toolkit.exit_with_failure_message_and_stats(
                context,
                usage_event,
                f"A checkpoint named `{checkpoint}` already exists. Please choose a new name.",
            )
    except InvalidTopLevelConfigKeyError as e:
        toolkit.exit_with_failure_message_and_stats(
            context, usage_event, f"<red>{e}</red>"
        )


def _write_checkpoint_to_disk(
    context: DataContext, checkpoint: Dict, checkpoint_name: str
) -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    checkpoint_dir = os.path.join(
        context.root_directory,
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
    )
    checkpoint_file = os.path.join(checkpoint_dir, f"{checkpoint_name}.yml")
    os.makedirs(checkpoint_dir, exist_ok=True)
    with open(checkpoint_file, "w") as f:
        yaml.dump(checkpoint, f)
    return checkpoint_file


def _load_checkpoint_yml_template() -> dict:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    template_file = file_relative_path(
        __file__, os.path.join("..", "data_context", "checkpoint_template.yml")
    )
    with open(template_file) as f:
        template = yaml.load(f)
    return template


@checkpoint.command(name="list")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_list(directory) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "List configured checkpoints. (Experimental)"
    context = toolkit.load_data_context_with_error_handling(directory)
    checkpoints = context.list_checkpoints()
    if not checkpoints:
        cli_message(
            "No checkpoints found.\n  - Use the command `great_expectations checkpoint new` to create one."
        )
        send_usage_message(
            data_context=context,
            event="cli.checkpoint.list",
            api_version="v2",
            success=True,
        )
        sys.exit(0)
    number_found = len(checkpoints)
    plural = "s" if (number_found > 1) else ""
    message = f"Found {number_found} checkpoint{plural}."
    pretty_list = [f" - <cyan>{cp}</cyan>" for cp in checkpoints]
    cli_message_list(pretty_list, list_intro_string=message)
    send_usage_message(
        data_context=context,
        event="cli.checkpoint.list",
        api_version="v2",
        success=True,
    )


@checkpoint.command(name="run")
@click.argument("checkpoint")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_run(checkpoint, directory) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Run a checkpoint. (Experimental)"
    usage_event = "cli.checkpoint.run"
    context = toolkit.load_data_context_with_error_handling(
        directory=directory, from_cli_upgrade_command=False
    )
    checkpoint: Checkpoint = toolkit.load_checkpoint(context, checkpoint, usage_event)
    try:
        results = checkpoint.run()
    except Exception as e:
        toolkit.exit_with_failure_message_and_stats(
            context, usage_event, f"<red>{e}</red>"
        )
    if not results["success"]:
        cli_message("Validation failed!")
        send_usage_message(
            data_context=context, event=usage_event, api_version="v2", success=True
        )
        print_validation_operator_results_details(results)
        sys.exit(1)
    cli_message("Validation succeeded!")
    send_usage_message(
        data_context=context, event=usage_event, api_version="v2", success=True
    )
    print_validation_operator_results_details(results)
    sys.exit(0)


def print_validation_operator_results_details(
    results: ValidationOperatorResult,
) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    max_suite_display_width = 40
    toolkit.cli_message(
        f"""
{'Suite Name'.ljust(max_suite_display_width)}     Status     Expectations met"""
    )
    for (id, result) in results.run_results.items():
        vr = result["validation_result"]
        stats = vr.statistics
        passed = stats["successful_expectations"]
        evaluated = stats["evaluated_expectations"]
        percentage_slug = (
            f"{(round(((passed / evaluated) * 100), 2) if (evaluated > 0) else 100)} %"
        )
        stats_slug = f"{passed} of {evaluated} ({percentage_slug})"
        if vr.success:
            status_slug = "<green>✔ Passed</green>"
        else:
            status_slug = "<red>✖ Failed</red>"
        suite_name = str(vr.meta["expectation_suite_name"])
        if len(suite_name) > max_suite_display_width:
            suite_name = suite_name[0:max_suite_display_width]
            suite_name = f"{suite_name[:(- 1)]}…"
        status_line = f"- {suite_name.ljust(max_suite_display_width)}   {status_slug}   {stats_slug}"
        toolkit.cli_message(status_line)


@checkpoint.command(name="script")
@click.argument("checkpoint")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_script(checkpoint, directory) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    Create a python script to run a checkpoint. (Experimental)\n\n    Checkpoints can be run directly without this script using the\n    `great_expectations checkpoint run` command.\n\n    This script is provided for those who wish to run checkpoints via python.\n    "
    context = toolkit.load_data_context_with_error_handling(directory)
    usage_event = "cli.checkpoint.script"
    _ = toolkit.load_checkpoint(context, checkpoint, usage_event)
    script_name = f"run_{checkpoint}.py"
    script_path = os.path.join(
        context.root_directory, context.GE_UNCOMMITTED_DIR, script_name
    )
    if os.path.isfile(script_path):
        toolkit.exit_with_failure_message_and_stats(
            context,
            usage_event,
            f"""<red>Warning! A script named {script_name} already exists and this command will not overwrite it.</red>
  - Existing file path: {script_path}""",
        )
    _write_checkpoint_script_to_disk(context.root_directory, checkpoint, script_path)
    cli_message(
        f"""<green>A python script was created that runs the checkpoint named: `{checkpoint}`</green>
  - The script is located in `great_expectations/uncommitted/run_{checkpoint}.py`
  - The script can be run with `python great_expectations/uncommitted/run_{checkpoint}.py`"""
    )
    send_usage_message(
        data_context=context, event=usage_event, api_version="v2", success=True
    )


def _load_script_template() -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    with open(file_relative_path(__file__, "checkpoint_script_template.py")) as f:
        template = f.read()
    return template


def _write_checkpoint_script_to_disk(
    context_directory: str, checkpoint_name: str, script_path: str
) -> None:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    script_full_path = os.path.abspath(os.path.join(script_path))
    template = _load_script_template().format(checkpoint_name, context_directory)
    linted_code = lint_code(template)
    with open(script_full_path, "w") as f:
        f.write(linted_code)
