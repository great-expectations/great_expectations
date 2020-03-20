import re
import sys

import six

from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger

try:
    from termcolor import colored
except ImportError:
    colored = None


def cli_message(string):
    # the DOTALL flag means that `.` includes newlines for multiline comments inside these tags
    flags = re.DOTALL
    mod_string = re.sub(
        "<blue>(.*?)</blue>", colored("\g<1>", "blue"), string, flags=flags
    )
    mod_string = re.sub(
        "<cyan>(.*?)</cyan>", colored("\g<1>", "cyan"), mod_string, flags=flags
    )
    mod_string = re.sub(
        "<green>(.*?)</green>", colored("\g<1>", "green"), mod_string, flags=flags
    )
    mod_string = re.sub(
        "<yellow>(.*?)</yellow>", colored("\g<1>", "yellow"), mod_string, flags=flags
    )
    mod_string = re.sub(
        "<red>(.*?)</red>", colored("\g<1>", "red"), mod_string, flags=flags
    )

    six.print_(colored(mod_string))


def is_sane_slack_webhook(url):
    """Really basic sanity checking."""
    if url is None:
        return False

    return "https://hooks.slack.com/" in url.strip()


def load_expectation_suite(context, suite_name):
    """
    Load an expectation suite from a given context.

    Handles a suite name with or without `.json`
    """
    if suite_name.endswith(".json"):
        suite_name = suite_name[:-5]
    try:
        suite = context.get_expectation_suite(suite_name)
        return suite
    except ge_exceptions.DataContextError as e:
        cli_message(
            f"<red>Could not find a suite named `{suite_name}`.</red> Please check "
            "the name by running `great_expectations suite list` and try again."
        )
        logger.info(e)
        sys.exit(1)
