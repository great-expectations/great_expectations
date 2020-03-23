import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.util import cli_message


@click.group()
def docs():
    """data docs operations"""
    pass


@docs.command(name="build")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--site-name",
    "-s",
    help="The site for which to generate documentation. See data_docs section in great_expectations.yml",
)
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True,
)
def docs_build(directory, site_name, view=True):
    """Build Data Docs for a project."""
    try:
        context = DataContext(directory)
        build_docs(context, site_name=site_name, view=view)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(1)
    except ge_exceptions.PluginModuleNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.PluginClassNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)


@docs.command(name="list")
@click.option(
    '--directory',
    '-d',
    default=None,
    help="The project's great_expectations directory."
)
def docs_list(directory):
    """List known Data Docs Sites."""
    try:
        context = DataContext(directory)
        docs_sites_url_dicts = context.get_docs_sites_urls()
        docs_sites_strings = [
            " - <cyan>{}</cyan>: {}".format(docs_site_dict["site_name"], docs_site_dict["site_url"])\
            for docs_site_dict in docs_sites_url_dicts
        ]

        if len(docs_sites_strings) == 0:
            cli_message("No Data Docs sites found")
            return

        if len(docs_sites_strings) == 1:
            list_intro_string = "1 Data Docs site found:"

        if len(docs_sites_strings) > 1:
            list_intro_string = "{} Data Docs sites found:".format(len(docs_sites_strings))
        cli_message_list(docs_sites_strings, list_intro_string)

    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return


def build_docs(context, site_name=None, view=True):
    """Build documentation in a context"""
    logger.debug("Starting cli.datasource.build_docs")

    cli_message("Building Data Docs...")

    if site_name is not None:
        site_names = [site_name]
    else:
        site_names = None

    index_page_locator_infos = context.build_data_docs(site_names=site_names)

    msg = "The following Data Docs sites were built:\n"
    for site_name, index_page_locator_info in index_page_locator_infos.items():
        if os.path.isfile(index_page_locator_info):
            msg += "- " + site_name + ":\n"
            msg += "   <green>file://" + index_page_locator_info + "</green>\n"
        else:
            msg += site_name + "\n"

    msg = msg.rstrip("\n")
    cli_message(msg)

    if view:
        context.open_data_docs()
