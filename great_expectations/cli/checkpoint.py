import os
import sys

import click
from ruamel.yaml import YAML
from sqlalchemy.exc import SQLAlchemyError

from great_expectations import DataContext
from great_expectations.cli import toolkit
from great_expectations.cli.datasource import (
    select_datasource,
)
from great_expectations.cli.mark import Mark as mark
from great_expectations.cli.util import (
    cli_message,
    cli_message_list,
    load_data_context_with_error_handling,
    load_expectation_suite,
)
from great_expectations.core import ExpectationSuite
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import (
    CheckpointError,
    CheckpointNotFoundError,
    DataContextError,
)
from great_expectations.util import lint_code

yaml = YAML()


@click.group()
def checkpoint():
    """Checkpoint operations"""
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
def checkpoint_new(checkpoint, directory, suite, datasource):
    """Create a new checkpoint for easy deployments. (Experimental)"""
    suite_name = suite
    usage_event = "cli.checkpoint.new"
    context = load_data_context_with_error_handling(directory)
    if checkpoint in context.list_checkpoints():
        _exit_with_failure_message(
            context,
            usage_event,
            f"A checkpoint named `{checkpoint}` already exists. Please choose a new name.",
        )

    # TODO select datasource
    datasource = toolkit.select_datasource(context)
    if datasource is None:
        send_usage_message(context, usage_event, success=False)
        sys.exit(1)

    checkpoint_file = os.path.join(
        context.root_directory, context.CHECKPOINTS_DIR, f"{checkpoint}.yml"
    )
    # TODO get batch kwargs
    _, _, _, batch_kwargs = toolkit.get_batch_kwargs(context, datasource.name)

    # TODO load suite
    suite: ExpectationSuite = context.get_expectation_suite(suite_name)

    # TODO load template with docs
    # TODO this should be the responsibility of the DataContext
    template_file = file_relative_path(
        __file__, os.path.join("..", "data_context", "checkpoint_template.yml")
    )
    with open(template_file, "r") as f:
        template = yaml.load(f)
    # TODO modify template
    template["batches"] = [
        {
            "batch_kwargs": batch_kwargs,
            "expectation_suite_names": [suite.expectation_suite_name],
        }
    ]
    # TODO write template as yml in dir
    with open(checkpoint_file, "w") as f:
        template = yaml.dump(template, f)

    # TODO show how to run
    # TODO print yay message
    cli_message(
        f"Yay! A checkpoint `{checkpoint}` was added to your project. To edit this..."
    )


def _checkpoint_new(
    suite, checkpoint_filename, directory, usage_event, datasource=None
):
    context = load_data_context_with_error_handling(directory)
    try:
        _validate_checkpoint_filename(checkpoint_filename)
        context_directory = context.root_directory
        datasource = _get_datasource(context, datasource)
        suite = load_expectation_suite(context, suite, usage_event)
        _, _, _, batch_kwargs = get_batch_kwargs(context, datasource.name)

        checkpoint_filename = _write_tap_file_to_disk(
            batch_kwargs, context_directory, suite, checkpoint_filename
        )
        cli_message(
            f"""\
<green>A new checkpoint has been generated!</green>
To run this checkpoint, run: <green>python {checkpoint_filename}</green>
You can edit this script or place this code snippet in your pipeline."""
        )
        send_usage_message(context, event=usage_event, success=True)
    except Exception as e:
        send_usage_message(context, event=usage_event, success=False)
        raise e


@checkpoint.command(name="list")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_list(directory):
    """Run a checkpoint. (Experimental)"""
    context = load_data_context_with_error_handling(directory)

    checkpoints = context.list_checkpoints()
    if not checkpoints:
        cli_message("No checkpoints found.")
        send_usage_message(context, event="cli.checkpoint.list", success=True)
        sys.exit(0)

    number_found = len(checkpoints)
    plural = "s" if number_found > 1 else ""
    message = f"Found {number_found} checkpoint{plural}."
    cli_message_list(checkpoints, list_intro_string=message)
    send_usage_message(context, event="cli.checkpoint.list", success=True)


@checkpoint.command(name="run")
@click.argument("checkpoint")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_run(checkpoint, directory):
    """Run a checkpoint. (Experimental)"""
    context = load_data_context_with_error_handling(directory)
    usage_event = "cli.checkpoint.run"

    checkpoint_config = {}
    # TODO factor down into toolkit
    try:
        checkpoint_config = context.get_checkpoint(checkpoint)
    except CheckpointNotFoundError as e:
        _exit_with_failure_message(
            context,
            usage_event,
            f"""\
<red>Could not find checkpoint `{checkpoint}`.</red> Try running:
  - `<green>great_expectations checkpoint list</green>` to verify your checkpoint exists
  - `<green>great_expectations checkpoint new</green>` to configure a new checkpoint""",
        )
    except CheckpointError as e:
        _exit_with_failure_message(context, usage_event, f"<red>{e}</red>")
    checkpoint_file = f"great_expectations/checkpoints/{checkpoint}.yml"

    batches_to_validate = []
    for batch in checkpoint_config["batches"]:
        _validate_at_least_one_suite_is_listed(context, batch, checkpoint_file)
        batch_kwargs = batch["batch_kwargs"]
        for suite_name in batch["expectation_suite_names"]:
            suite = load_expectation_suite(context, suite_name, usage_event)
            # TODO maybe move into toolkit utility
            try:
                batch = toolkit.load_batch(context, suite, batch_kwargs)
            except (FileNotFoundError, SQLAlchemyError, IOError, DataContextError) as e:
                _exit_with_failure_message(
                    context,
                    usage_event,
                    f"""<red>There was a problem loading a batch:
  - Batch: {batch_kwargs}
  - {e}
  - Please verify these batch kwargs in the checkpoint file: `{checkpoint_file}`</red>""",
                )
            batches_to_validate.append(batch)
    try:
        results = context.run_validation_operator(
            checkpoint_config["validation_operator_name"],
            assets_to_validate=batches_to_validate,
            # TODO prepare for new RunID - checkpoint name and timestamp
            # run_id=RunID(checkpoint)
        )
    except DataContextError as e:
        _exit_with_failure_message(context, usage_event, f"<red>{e}</red>")

    if not results["success"]:
        # TODO maybe more verbose output (n of n passed)
        cli_message("Validation Failed!")
        send_usage_message(context, event=usage_event, success=True)
        sys.exit(1)

    # TODO maybe more verbose output (n of n passed)
    cli_message("Validation Succeeded!")
    send_usage_message(context, event=usage_event, success=True)
    sys.exit(0)


def _validate_at_least_one_suite_is_listed(context, batch, checkpoint_file):
    batch_kwargs = batch["batch_kwargs"]
    suites = batch["expectation_suite_names"]
    if not suites:
        _exit_with_failure_message(
            context,
            "cli.checkpoint.run",
            f"""<red>A batch has no suites associated with it. At least one suite is required.
  - Batch: {batch_kwargs}
  - Please add at least one suite to your checkpoint file: {checkpoint_file}</red>""",
        )


def _exit_with_failure_message(
    context: DataContext, usage_event: str, message: str
) -> None:
    cli_message(message)
    send_usage_message(context, event=usage_event, success=False)
    sys.exit(1)


def _validate_checkpoint_filename(checkpoint_filename):
    if not checkpoint_filename.endswith(".py"):
        cli_message(
            "<red>Tap filename must end in .py. Please correct and re-run</red>"
        )
        sys.exit(1)


def _get_datasource(context, datasource):
    datasource = select_datasource(context, datasource_name=datasource)
    if not datasource:
        cli_message("<red>No datasources found in the context.</red>")
        sys.exit(1)
    return datasource


def _load_template():
    with open(file_relative_path(__file__, "checkpoint_template.py")) as f:
        template = f.read()
    return template


def _write_tap_file_to_disk(
    batch_kwargs, context_directory, suite, checkpoint_filename
):
    tap_file_path = os.path.abspath(
        os.path.join(context_directory, "..", checkpoint_filename)
    )

    template = _load_template().format(
        checkpoint_filename,
        context_directory,
        suite.expectation_suite_name,
        batch_kwargs,
    )
    linted_code = lint_code(template)
    with open(tap_file_path, "w") as f:
        f.write(linted_code)

    return tap_file_path
