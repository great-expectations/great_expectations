import os
import subprocess

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.suite import _load_suite
from great_expectations.cli.util import cli_message
from great_expectations.data_context.util import file_relative_path


@click.group()
def tap():
    """tap operations"""
    pass


@tap.command(name="new")
@click.argument("suite")
@click.argument("tap_filename")
@click.option("--csv", default=None)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
def tap_new(suite, tap_filename, directory, csv=None):
    """Create a new tap."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    directory = context.root_directory

    suite = _load_suite(context, suite)
    n_expectations = len(suite.expectations)
    cli_message(f"Loaded suite {suite.expectation_suite_name} that has {n_expectations} expectations.")

    template = _load_template()
    batch_kwargs = {"datasource": "files_datasource", "path": os.path.abspath(csv), "reader_method": "read_csv"}
    template = template.format(tap_filename, directory, suite.expectation_suite_name, batch_kwargs)

    cli_message(f"<yellow>{template}</yellow>")

    with open(tap_filename, "w") as f:
        f.write(template)
    cli_message(f"""<green>A new tap has been made! Open {tap_filename} in an editor to tweak it</green>""")

    # _debugging_stuff(tap_filename)


def _debugging_stuff(tap_filename):
    print("\n\n")
    subprocess.call(["cat", tap_filename])
    print("\n\n")
    subprocess.call(["python", tap_filename])


def _load_template():
    with open(file_relative_path(__file__, "tap_template.py"), "r") as f:
        template = f.read()
    return template
