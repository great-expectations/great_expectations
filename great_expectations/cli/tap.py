import click

from great_expectations.cli.checkpoint import _checkpoint_new
from great_expectations.cli.mark import Mark as mark


@click.group()
def tap():
    """Tap operations"""
    pass


@tap.command(name="new")
@click.argument("suite")
@click.argument("tap_filename")
@click.option("--datasource", default=None)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_deprecation(
    "<yellow>This command has been renamed `checkpoint new` and will be removed in the next major release.</yellow>"
)
def tap_new(suite, tap_filename, directory, datasource=None):
    """Create a new tap file for easy deployments."""
    _checkpoint_new(
        suite, tap_filename, directory, usage_event="cli.tap.new", datasource=datasource
    )
