# -*- coding: utf-8 -*-
import logging

import click

from great_expectations import __version__ as ge_version
from great_expectations.cli.cli_logging import _set_up_logger, logger
from great_expectations.cli.datasource import datasource
from great_expectations.cli.docs import docs
from great_expectations.cli.init import init
from great_expectations.cli.project import project
from great_expectations.cli.suite import suite


# TODO: consider using a specified-order supporting class for help (but wasn't working with python 2)
@click.group()
@click.version_option(version=ge_version)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Set great_expectations to use verbose output.",
)
def cli(verbose):
    """
Welcome to the great_expectations CLI!

Most commands follow this format: great_expectations <NOUN> <VERB>

The nouns are: datasource, docs, project, suite

Most nouns accept the following verbs: new, list, edit

In addition, the CLI supports the following special commands:

- great_expectations init : same as `project new`

- great_expectations datasource profile : profile a  datasource

- great_expectations docs build : compile documentation from expectations
"""
    _set_up_logger()
    if verbose:
        # Note we are explicitly not using a logger in all CLI output to have
        # more control over console UI.
        logger.setLevel(logging.DEBUG)


cli.add_command(datasource)
cli.add_command(docs)
cli.add_command(init)
cli.add_command(project)
cli.add_command(suite)


def main():
    cli()


if __name__ == "__main__":
    main()
