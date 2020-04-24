import os
import sys

import click

from great_expectations.cli.datasource import (
    get_batch_kwargs,
    select_datasource,
)
from great_expectations.cli.util import (
    cli_message,
    cli_message_list,
    load_data_context_with_error_handling,
    load_expectation_suite,
)
from great_expectations.cli.mark import Mark as mark
from great_expectations.core.usage_statistics.usage_statistics import (
    send_usage_message,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import lint_code


@click.group()
def checkpoint():
    """Checkpoint operations"""
    pass


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
        sys.exit(0)

    number_found = len(checkpoints)
    plural = "s" if number_found > 1 else ""
    message = f"Found {number_found} checkpoint{plural}."
    cli_message_list(checkpoints, list_intro_string=message)


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


def _write_tap_file_to_disk(batch_kwargs, context_directory, suite, checkpoint_filename):
    tap_file_path = os.path.abspath(os.path.join(context_directory, "..", checkpoint_filename))

    template = _load_template().format(
        checkpoint_filename, context_directory, suite.expectation_suite_name, batch_kwargs
    )
    linted_code = lint_code(template)
    with open(tap_file_path, "w") as f:
        f.write(linted_code)

    return tap_file_path
