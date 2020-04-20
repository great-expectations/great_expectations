import json
import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import cli_message, cli_message_list, cli_message_dict
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message


@click.group()
def store():
    """Store operations"""
    pass


@store.command(name="list")
@click.option(
    '--directory',
    '-d',
    default=None,
    help="The project's great_expectations directory."
)
def store_list(directory):
    """List known Stores."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    try:
        stores = context.list_stores()

        if len(stores) == 0:
            cli_message("No Stores found")
            send_usage_message(
                data_context=context,
                event="cli.store.list",
                success=True
            )
            return

        if len(stores) == 1:
            list_intro_string = "1 Store found:"

        if len(stores) > 1:
            list_intro_string = "{} Stores found:".format(len(stores))

        cli_message(list_intro_string)

        for store in stores:
            cli_message("")
            cli_message_dict(store)

        send_usage_message(
            data_context=context,
            event="cli.store.list",
            success=True
        )
    except Exception as e:
        send_usage_message(
            data_context=context,
            event="cli.store.list",
            success=False
        )
        raise e

