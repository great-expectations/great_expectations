import os
import re
import shutil
import sys

import click
import six

from great_expectations import DataContext
from great_expectations.cli.init_messages import (
    NEW_TEMPLATE_INSTALLED,
    NEW_TEMPLATE_PROMPT,
)

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


def _offer_to_install_new_template(err, ge_dir):
    ge_dir = os.path.abspath(ge_dir)
    cli_message("<red>{}</red>".format(err.message))
    ge_yml = os.path.join(ge_dir, DataContext.GE_YML)
    archived_yml = ge_yml + ".archive"

    if click.confirm(NEW_TEMPLATE_PROMPT.format(ge_yml, archived_yml), default=True):
        # archive existing project config
        shutil.move(ge_yml, archived_yml)
        DataContext.write_project_template_to_disk(ge_dir)

        cli_message(
            NEW_TEMPLATE_INSTALLED.format("file://" + ge_yml, "file://" + archived_yml)
        )
    else:
        cli_message(
            """\nOK. To continue, you will need to upgrade your config file to the latest format.
  - Please see the docs here: <blue>https://docs.greatexpectations.io/en/latest/reference/data_context_reference.html</blue>
  - We are super sorry about this breaking change! :]
  - If you are running into any problems, please reach out on Slack and we can
    help you in realtime: https://greatexpectations.io/slack"""
        )
    sys.exit(0)
