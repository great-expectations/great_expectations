from __future__ import annotations

import sys

import click

from great_expectations import exceptions as gx_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.cli_messages import SECTION_SEPARATOR
from great_expectations.cli.pretty_printing import cli_colorize_string, cli_message
from great_expectations.cli.toolkit import load_data_context_with_error_handling
from great_expectations.cli.upgrade_helpers import GE_UPGRADE_HELPER_VERSION_MAP
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import CURRENT_GX_CONFIG_VERSION


@click.group()
def project() -> None:
    """Project operations"""
    pass


@project.command(name="check-config")
@click.pass_context
def project_check_config(ctx: click.Context) -> None:
    """Check a config for validity and help with migrations."""
    cli_message("Checking your config files for validity...\n")
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")

    is_config_ok, error_message, context = do_config_check(directory)

    if not (is_config_ok and context):
        cli_message("Unfortunately, your config appears to be invalid:\n")
        cli_message(f"<red>{error_message}</red>")
        sys.exit(1)

    send_usage_message(
        data_context=context,
        event=UsageStatsEvents.CLI_PROJECT_CHECK_CONFIG,
        success=True,
    )

    cli_message("<green>Your config file appears valid!</green>")


@project.command(name="upgrade")
@click.pass_context
def project_upgrade(ctx: click.Context) -> None:
    """Upgrade a project after installing the next Great Expectations major version."""
    cli_message("\nChecking project...")
    cli_message(SECTION_SEPARATOR)
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")

    if load_data_context_with_error_handling(
        directory=directory, from_cli_upgrade_command=True
    ):
        sys.exit(0)
    else:
        failure_message = "Error: Your project could not be upgraded.\n"
        cli_message(f"<red>{failure_message}</red>")
        sys.exit(1)


def do_config_check(
    target_directory: str | None,
) -> tuple[bool, str, FileDataContext | None]:
    is_config_ok: bool = True
    upgrade_message: str = ""
    context: FileDataContext | None = None
    try:
        # Without this check, FileDataContext will possibly scaffold a project structure.
        # As we want CLI users to follow the `init` workflow, we should exit early if we can't find a context YAML.
        if not FileDataContext._find_context_yml_file(target_directory):
            raise gx_exceptions.ConfigNotFoundError()

        context = FileDataContext(context_root_dir=target_directory)
        ge_config_version: int = context.get_config().config_version  # type: ignore[union-attr] # could be dict, str
        if int(ge_config_version) < CURRENT_GX_CONFIG_VERSION:
            is_config_ok = False
            upgrade_message = f"""The config_version of your great_expectations.yml -- {float(ge_config_version)} -- is outdated.
Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and
upgrade your Great Expectations configuration to version {float(CURRENT_GX_CONFIG_VERSION)} in order to take advantage of the latest capabilities.
"""
            context = None
        elif int(ge_config_version) > CURRENT_GX_CONFIG_VERSION:
            raise gx_exceptions.UnsupportedConfigVersionError(
                f"""Invalid config version ({ge_config_version}).\n    The maximum valid version is \
{CURRENT_GX_CONFIG_VERSION}.
"""
            )
        else:
            upgrade_helper_class = GE_UPGRADE_HELPER_VERSION_MAP.get(
                int(ge_config_version)
            )
            if upgrade_helper_class:
                upgrade_helper = upgrade_helper_class(
                    data_context=context, update_version=False
                )
                manual_steps_required = upgrade_helper.manual_steps_required()
                if manual_steps_required:
                    (
                        upgrade_overview,
                        confirmation_required,
                    ) = upgrade_helper.get_upgrade_overview()
                    upgrade_overview = cli_colorize_string(upgrade_overview)
                    cli_message(string=upgrade_overview)
                    is_config_ok = False
                    upgrade_message = """The configuration of your great_expectations.yml is outdated.  Please \
consult the V3 API migration guide \
https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and upgrade your \
Great Expectations configuration in order to take advantage of the latest capabilities.
"""
                    context = None
    except (
        gx_exceptions.InvalidConfigurationYamlError,
        gx_exceptions.InvalidTopLevelConfigKeyError,
        gx_exceptions.MissingTopLevelConfigKeyError,
        gx_exceptions.InvalidConfigValueTypeError,
        gx_exceptions.UnsupportedConfigVersionError,
        gx_exceptions.DataContextError,
        gx_exceptions.PluginClassNotFoundError,
        gx_exceptions.PluginModuleNotFoundError,
        gx_exceptions.GreatExpectationsError,
    ) as err:
        is_config_ok = False
        upgrade_message = err.message
        context = None

    return (
        is_config_ok,
        upgrade_message,
        context,
    )
