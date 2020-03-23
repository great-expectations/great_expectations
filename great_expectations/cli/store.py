import json
import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import cli_message, cli_message_list


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
        stores = context.list_stores()

        if len(stores) == 0:
            cli_message("No Stores found")
            return

        if len(stores) == 1:
            list_intro_string = "1 Store found:"

        if len(stores) > 1:
            list_intro_string = "{} Stores found:".format(len(stores))

        cli_message(list_intro_string)
        for store in stores:
            cli_message("")
            cli_message(" - <cyan>name:</cyan> {}".format(store.get("name")))
            if store.get("class_name"):
                cli_message("   <cyan>class_name:</cyan> {}".format(store.get("class_name")))
            if store.get("store_backend"):
                store_backend = store.get("store_backend")
                cli_message("   <cyan>store_backend:</cyan>")
                cli_message("     <cyan>class_name:</cyan> {}".format(store_backend.pop("class_name")))
                for key, val in store_backend.items():
                    cli_message("     <cyan>{}:</cyan> {}".format(key, val))

    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return