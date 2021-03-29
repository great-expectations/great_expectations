import os
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.cli_messages import (
    GREETING,
    HOW_TO_CUSTOMIZE,
    LETS_BEGIN_PROMPT,
    ONBOARDING_COMPLETE,
    PROJECT_IS_COMPLETE,
    READY_FOR_CUSTOMIZATION,
    RUN_INIT_AGAIN,
    SECTION_SEPARATOR,
    SLACK_LATER,
    SLACK_SETUP_COMPLETE,
    SLACK_SETUP_INTRO,
    SLACK_SETUP_PROMPT,
    SLACK_WEBHOOK_PROMPT,
)
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.exceptions import (
    DataContextError,
    DatasourceInitializationError,
)
from great_expectations.util import is_sane_slack_webhook

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.command()
@click.option(
    # Note this --no-view option is mostly here for tests
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag.",
    default=True,
)
@click.option(
    "--usage-stats/--no-usage-stats",
    help="By default, usage statistics are enabled unless you specify the --no-usage-stats flag.",
    default=True,
)
@click.pass_context
def init(ctx, view, usage_stats):
    """
    Initialize a new Great Expectations project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    if directory is None:
        directory = os.getcwd()
    target_directory = os.path.abspath(directory)
    ge_dir = _get_full_path_to_ge_dir(target_directory)
    cli_message(GREETING)

    if DataContext.does_config_exist_on_disk(ge_dir):
        try:
            if DataContext.is_project_initialized(ge_dir):
                # Ensure the context can be instantiated
                cli_message(PROJECT_IS_COMPLETE)
                sys.exit(0)
        except (DataContextError, DatasourceInitializationError) as e:
            cli_message("<red>{}</red>".format(e.message))
            sys.exit(1)

        try:
            context = DataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            cli_message(ONBOARDING_COMPLETE)
            # TODO if this is correct, ensure this is covered by a test
            # cli_message(SETUP_SUCCESS)
            # exit(0)
        except DataContextError as e:
            cli_message("<red>{}</red>".format(e.message))
            # TODO ensure this is covered by a test
            exit(5)
    else:
        if not ctx.obj.assume_yes:
            if not click.confirm(LETS_BEGIN_PROMPT, default=True):
                cli_message(RUN_INIT_AGAIN)
                # TODO ensure this is covered by a test
                exit(0)

        try:
            context = DataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            toolkit.send_usage_message(
                data_context=context, event="cli.init.create", success=True
            )
        except DataContextError as e:
            # TODO ensure this is covered by a test
            cli_message("<red>{}</red>".format(e))

    cli_message(SECTION_SEPARATOR)
    cli_message(READY_FOR_CUSTOMIZATION)
    cli_message(HOW_TO_CUSTOMIZE)
    sys.exit(0)


def _slack_setup(context):
    webhook_url = None
    cli_message(SLACK_SETUP_INTRO)
    if not click.confirm(SLACK_SETUP_PROMPT, default=True):
        cli_message(SLACK_LATER)
        return context
    else:
        webhook_url = click.prompt(SLACK_WEBHOOK_PROMPT, default="")

    while not is_sane_slack_webhook(webhook_url):
        cli_message("That URL was not valid.\n")
        if not click.confirm(SLACK_SETUP_PROMPT, default=True):
            cli_message(SLACK_LATER)
            return context
        webhook_url = click.prompt(SLACK_WEBHOOK_PROMPT, default="")

    context.save_config_variable("validation_notification_slack_webhook", webhook_url)
    cli_message(SLACK_SETUP_COMPLETE)

    return context


def _get_full_path_to_ge_dir(target_directory):
    return os.path.abspath(os.path.join(target_directory, DataContext.GE_DIR))
