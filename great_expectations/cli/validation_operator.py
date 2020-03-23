import json
import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import cli_message, cli_message_list


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
        # validation_operator_strings = [
        #     json.dumps(validation_operator_dict, indent=2) for validation_operator_dict in validation_operators
        # ]

        if len(validation_operators) == 0:
            cli_message("No Validation Operators found")
            return

        if len(validation_operators) == 1:
            list_intro_string = "1 Validation Operator found:"

        if len(validation_operators) > 1:
            list_intro_string = "{} Validation Operators found:".format(len(validation_operators))

        cli_message(list_intro_string)
        for validation_operator in validation_operators:
            cli_message(" - <cyan>name:</cyan> {}".format(validation_operator.pop("name")))
            if validation_operator.get("class_name"):
                class_name = validation_operator.pop("class_name")
                cli_message("    <cyan>class_name:</cyan> {}".format(class_name))
            if validation_operator.get("action_list"):
                action_list = validation_operator.pop("action_list")
                cli_message("    <cyan>action_list:</cyan> {}".format(action_list_to_string(action_list)))
            for key, val in validation_operator.items():
                cli_message("    <cyan>{}:</cyan> {}".format(key, val))

    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

def action_list_to_string(action_list):
    action_list_string = ""
    for idx, action in enumerate(action_list):
        action_list_string += "{} ({})".format(action["name"], action["action"]["class_name"])
        if idx == len(action_list) - 1:
            continue
        action_list_string += " => "
    return action_list_string