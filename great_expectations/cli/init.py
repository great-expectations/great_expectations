import os
import sys
import warnings

import click

from great_expectations import exceptions as gx_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.cli_messages import (
    COMPLETE_ONBOARDING_PROMPT,
    GREETING,
    HOW_TO_CUSTOMIZE,
    LETS_BEGIN_PROMPT,
    ONBOARDING_COMPLETE,
    PROJECT_IS_COMPLETE,
    READY_FOR_CUSTOMIZATION,
    RUN_INIT_AGAIN,
    SECTION_SEPARATOR,
)
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.compatibility import sqlalchemy
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.exceptions import (
    DataContextError,
    DatasourceInitializationError,
)

SQLAlchemyError = sqlalchemy.SQLAlchemyError
if not SQLAlchemyError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = gx_exceptions.ProfilerError


@click.command()
@click.option(
    "--usage-stats/--no-usage-stats",
    help="By default, usage statistics are enabled unless you specify the --no-usage-stats flag.",
    default=True,
)
@click.pass_context
def init(ctx: click.Context, usage_stats: bool) -> None:  # noqa: PLR0912
    """
    Initialize a new Great Expectations project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, creates a project file, and
    appends to a `.gitignore` file.
    """
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    if directory is None:
        directory = os.getcwd()  # noqa: PTH109
    target_directory = os.path.abspath(directory)  # noqa: PTH100
    ge_dir = _get_full_path_to_ge_dir(target_directory)
    cli_message(GREETING)

    if FileDataContext.does_config_exist_on_disk(ge_dir):
        message = f"""Warning. An existing `{FileDataContext.GX_YML}` was found here: {ge_dir}."""
        warnings.warn(message)
        try:
            project_file_structure_exists = FileDataContext.is_project_scaffolded(
                ge_dir
            )
            if project_file_structure_exists:
                cli_message(PROJECT_IS_COMPLETE)
                sys.exit(0)
            else:
                # Prompt to modify the project to add missing files
                if not ctx.obj.assume_yes:  # noqa: PLR5501
                    if not click.confirm(COMPLETE_ONBOARDING_PROMPT, default=True):
                        cli_message(RUN_INIT_AGAIN)
                        sys.exit(0)

        except (DataContextError, DatasourceInitializationError) as e:
            cli_message(f"<red>{e.message}</red>")
            sys.exit(1)

        try:
            FileDataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            cli_message(ONBOARDING_COMPLETE)

        except DataContextError as e:
            cli_message(f"<red>{e.message}</red>")
            # TODO ensure this is covered by a test
            sys.exit(5)
    else:
        if not ctx.obj.assume_yes:
            if not click.confirm(LETS_BEGIN_PROMPT, default=True):
                cli_message(RUN_INIT_AGAIN)
                sys.exit(0)

        try:
            context = FileDataContext.create(
                target_directory, usage_statistics_enabled=usage_stats
            )
            send_usage_message(
                data_context=context,
                event=UsageStatsEvents.CLI_INIT_CREATE,
                success=True,
            )
        except DataContextError as e:
            # TODO ensure this is covered by a test
            cli_message(f"<red>{e}</red>")

    cli_message(SECTION_SEPARATOR)
    cli_message(READY_FOR_CUSTOMIZATION)
    cli_message(HOW_TO_CUSTOMIZE)
    sys.exit(0)


def _get_full_path_to_ge_dir(target_directory: str) -> str:
    return os.path.abspath(  # noqa: PTH100
        os.path.join(target_directory, FileDataContext.GX_DIR)  # noqa: PTH118
    )
