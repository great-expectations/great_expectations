import sys

import click
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.util import cli_message
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message


@click.group()
def project():
    """Project operations"""
    pass


@project.command(name="check-config")
@click.option(
    "--directory",
    "-d",
    default="./great_expectations",
    help="The project's great_expectations directory.",
)
def project_check_config(directory):
    """Check a config for validity and help with migrations."""
    cli_message("Checking your config files for validity...\n")
    is_config_ok, error_message, context = do_config_check(directory)
    if context:
        send_usage_message(
            data_context=context, event="cli.project.check_config", success=True
        )
    if not is_config_ok:
        cli_message("Unfortunately, your config appears to be invalid:\n")
        cli_message("<red>{}</red>".format(error_message))
        sys.exit(1)

    cli_message("<green>Your config file appears valid!</green>")


def do_config_check(target_directory):
    try:
        context = DataContext(context_root_dir=target_directory)
        return True, None, context
    except (
        ge_exceptions.InvalidConfigurationYamlError,
        ge_exceptions.InvalidTopLevelConfigKeyError,
        ge_exceptions.MissingTopLevelConfigKeyError,
        ge_exceptions.InvalidConfigValueTypeError,
        ge_exceptions.UnsupportedConfigVersionError,
        ge_exceptions.DataContextError,
        ge_exceptions.PluginClassNotFoundError,
        ge_exceptions.PluginModuleNotFoundError,
        ge_exceptions.GreatExpectationsError,
    ) as err:
        return False, err.message, None
