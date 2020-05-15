import os
import sys

import click
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.datasource import add_datasource as add_datasource_impl
from great_expectations.cli.docs import build_docs
from great_expectations.cli.init_messages import (
    BUILD_DOCS_PROMPT,
    GREETING,
    LETS_BEGIN_PROMPT,
    ONBOARDING_COMPLETE,
    PROJECT_IS_COMPLETE,
    RUN_INIT_AGAIN,
    SETUP_SUCCESS,
    SLACK_LATER,
    SLACK_SETUP_COMPLETE,
    SLACK_SETUP_INTRO,
    SLACK_SETUP_PROMPT,
    SLACK_WEBHOOK_PROMPT,
)
from great_expectations.cli.util import cli_message, is_sane_slack_webhook
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message
from great_expectations.exceptions import (
    DataContextError,
    DatasourceInitializationError,
)

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.command()
@click.option(
    "--target-directory",
    "-d",
    default="./",
    help="The root of the project directory where you want to initialize Great Expectations.",
)
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
def init(target_directory, view, usage_stats):
    """
    Initialize a new Great Expectations project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    target_directory = os.path.abspath(target_directory)
    ge_dir = _get_full_path_to_ge_dir(target_directory)
    cli_message(GREETING)

    if DataContext.does_config_exist_on_disk(ge_dir):
        try:
            if DataContext.is_project_initialized(ge_dir):
                # Ensure the context can be instantiated
                cli_message(PROJECT_IS_COMPLETE)
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
        if not click.confirm(LETS_BEGIN_PROMPT, default=True):
            cli_message(RUN_INIT_AGAIN)
            # TODO ensure this is covered by a test
            exit(0)

        try:
            context = DataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            send_usage_message(
                data_context=context, event="cli.init.create", success=True
            )
        except DataContextError as e:
            # TODO ensure this is covered by a test
            cli_message("<red>{}</red>".format(e))

    try:
        # if expectations exist, offer to build docs
        context = DataContext(ge_dir)
        if context.list_expectation_suites():
            if click.confirm(BUILD_DOCS_PROMPT, default=True):
                build_docs(context, view=view)

        else:
            datasources = context.list_datasources()
            if len(datasources) == 0:
                datasource_name, data_source_type = add_datasource_impl(
                    context, choose_one_data_asset=True
                )
                if not datasource_name:  # no datasource was created
                    sys.exit(1)

            datasources = context.list_datasources()
            if len(datasources) == 1:
                datasource_name = datasources[0]["name"]

                success, suite_name = toolkit.create_expectation_suite(
                    context,
                    datasource_name=datasource_name,
                    additional_batch_kwargs={"limit": 1000},
                    open_docs=view,
                )
                if success:
                    cli_message(
                        "A new Expectation suite '{}' was added to your project".format(
                            suite_name
                        )
                    )

                cli_message(SETUP_SUCCESS)
                sys.exit(0)
    except (
        DataContextError,
        ge_exceptions.ProfilerError,
        IOError,
        SQLAlchemyError,
    ) as e:
        cli_message("<red>{}</red>".format(e))
        sys.exit(1)


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
