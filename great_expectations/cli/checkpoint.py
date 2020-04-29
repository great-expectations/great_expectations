import os
import sys

import click

from great_expectations.cli.datasource import (
    get_batch_kwargs,
    select_datasource,
)
from great_expectations.cli.util import (
    cli_message,
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


@checkpoint.command(name="new")
@click.argument("suite")
@click.argument("checkpoint_filename")
@click.option("--datasource", default=None)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def checkpoint_new(suite, checkpoint_filename, directory, datasource=None):
    """Create a new checkpoint file for easy deployments. (Experimental)"""
    _checkpoint_new(suite, checkpoint_filename, directory, usage_event="cli.checkpoint.new", datasource=datasource)


def _checkpoint_new(suite, checkpoint_filename, directory, usage_event, datasource=None):
    context = load_data_context_with_error_handling(directory)
    try:
        _validate_checkpoint_filename(checkpoint_filename)
        context_directory = context.root_directory
        datasource = _get_datasource(context, datasource)
        suite = load_expectation_suite(context, suite)
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
        send_usage_message(
            data_context=context,
            event=usage_event,
            success=True
        )
    except Exception as e:
        send_usage_message(
            data_context=context,
            event=usage_event,
            success=False
        )
        raise e


def _validate_checkpoint_filename(checkpoint_filename):
    if not checkpoint_filename.endswith(".py"):
        cli_message(
            "<red>Tap filename must end in .py. Please correct and re-run</red>"
        )
        exit(1)


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
