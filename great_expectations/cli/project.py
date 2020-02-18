import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.util import (
    _offer_to_install_new_template,
    cli_message,
)


@click.group()
def project():
    """project operations"""
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

    try:
        is_config_ok, error_message = do_config_check(directory)
        if is_config_ok:
            cli_message("<green>Your config file appears valid!</green>")
        else:
            cli_message("Unfortunately, your config appears to be invalid:\n")
            cli_message("<red>{}</red>".format(error_message))
            sys.exit(1)
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, directory)


def do_config_check(target_directory):
    try:
        DataContext(context_root_dir=target_directory)
        return True, None
    except (
        ge_exceptions.InvalidConfigurationYamlError,
        ge_exceptions.InvalidTopLevelConfigKeyError,
        ge_exceptions.MissingTopLevelConfigKeyError,
        ge_exceptions.InvalidConfigValueTypeError,
        ge_exceptions.UnsupportedConfigVersionError,
        ge_exceptions.DataContextError,
        ge_exceptions.PluginClassNotFoundError,
    ) as err:
        return False, err.message
