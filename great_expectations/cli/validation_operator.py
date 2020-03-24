import json
import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import cli_message, cli_message_list, cli_message_dict


@click.group()
def validation_operator():
    """Validation Operator operations"""
    pass


@validation_operator.command(name="list")
@click.option(
    '--directory',
    '-d',
    default=None,
    help="The project's great_expectations directory."
)
def validation_operator_list(directory):
    """List known datasources."""
    try:
        context = DataContext(directory)
        validation_operators = context.list_validation_operators()

        if len(validation_operators) == 0:
            cli_message("No Validation Operators found")
            return

        if len(validation_operators) == 1:
            list_intro_string = "1 Validation Operator found:"

        if len(validation_operators) > 1:
            list_intro_string = "{} Validation Operators found:".format(len(validation_operators))

        cli_message(list_intro_string)
        for validation_operator in validation_operators:
            cli_message("")
            cli_message_dict(validation_operator)

    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return


