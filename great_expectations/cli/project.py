import sys

from typing import Optional

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.cli_messages import SECTION_SEPARATOR
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.cli.toolkit import load_data_context_with_error_handling
from great_expectations.data_context.types.base import CURRENT_GE_CONFIG_VERSION


@click.group()
def project():
    """Project operations"""
    pass


@project.command(name="check-config")
@click.pass_context
def project_check_config(ctx):
    """Check a config for validity and help with migrations."""
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_CHECK_CONFIG] CTX: {ctx} ; TYPE: {str(type(ctx))}')
    cli_message("Checking your config files for validity...\n")
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_CHECK_CONFIG] DIRECTORY: {directory} ; TYPE: {str(type(directory))}')
    is_config_ok, error_message, context = do_config_check(directory)
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_CHECK_CONFIG] IS_CONFIG_OK: {is_config_ok} ; TYPE: {str(type(is_config_ok))}')
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_CHECK_CONFIG] ERROR_MESSAGE: {error_message} ; TYPE: {str(type(error_message))}')
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_CHECK_CONFIG] CONTEXT: {context} ; TYPE: {str(type(context))}')
    if context:
        toolkit.send_usage_message(
            data_context=context, event="cli.project.check_config", success=True
        )
    if not is_config_ok:
        cli_message("Unfortunately, your config appears to be invalid:\n")
        cli_message(f"<red>{error_message}</red>")
        sys.exit(1)

    cli_message("<green>Your config file appears valid!</green>")


@project.command(name="upgrade")
@click.pass_context
def project_upgrade(ctx):
    """Upgrade a project after installing the next Great Expectations major version."""
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_UPGRADE] CTX: {ctx} ; TYPE: {str(type(ctx))}')
    cli_message("\nChecking project...")
    cli_message(SECTION_SEPARATOR)
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    print(f'\n[ALEX_TEST] [CLI_V3.PROJECT_UPGRADE] DIRECTORY: {directory} ; TYPE: {str(type(directory))}')
    if load_data_context_with_error_handling(
        directory=directory, from_cli_upgrade_command=True
    ):
        up_to_date_message = (
            "Your project is up-to-date - no further upgrade is necessary.\n"
        )
        cli_message(f"<green>{up_to_date_message}</green>")
        sys.exit(0)


def do_config_check(target_directory):
    print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] TARGET_DIRECTORY: {target_directory} ; TYPE: {str(type(target_directory))}')
    try:
        print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] GETTING_DATA_CONTEXT_FOR_TARGET_DIRECTORY: {target_directory} ; TYPE: {str(type(target_directory))}')
        context: Optional[DataContext] = DataContext(context_root_dir=target_directory)
        print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] CONTEXT: {context} ; TYPE: {str(type(context))}')
        ge_config_version: int = context.get_config().config_version
        print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] GE_CONFIG_VERSION: {ge_config_version} ; TYPE: {str(type(ge_config_version))}')
        print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] CURRENT_GE_CONFIG_VERSION: {CURRENT_GE_CONFIG_VERSION} ; TYPE: {str(type(CURRENT_GE_CONFIG_VERSION))}')
        if int(ge_config_version) < CURRENT_GE_CONFIG_VERSION:
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX -- update document link</Alex>
            upgrade_message: str = f"""The config_version of your great_expectations.yml -- {float(ge_config_version)} -- is outdated.
Please consult the 0.13.x migration guide https://docs.greatexpectations.io/en/latest/guides/how_to_guides/migrating_versions.html and
upgrade your Great Expectations configuration to version {float(CURRENT_GE_CONFIG_VERSION)} in order to take advantage of the latest capabilities.
"""
            # TODO: <Alex>ALEX</Alex>
            return (
                False,
                upgrade_message,
                None,
            )
        elif int(ge_config_version) > CURRENT_GE_CONFIG_VERSION:
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX - raise exception -- error message; this must never happen</Alex>
            raise ge_exceptions.UnsupportedConfigVersionError(
                f"""Invalid config version ({ge_config_version}).\n    The maximum valid version is \
{CURRENT_GE_CONFIG_VERSION}.
"""
            )
            # TODO: <Alex>ALEX</Alex>
        else:
            # TODO: <Alex>ALEX</Alex>
            # TODO: <Alex>ALEX - check for datasources, validation operators; this must never happen</Alex>
            print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] MUST_DO_SOMETHING--GE_CONFIG_VERSION: {ge_config_version} ; TYPE: {str(type(ge_config_version))}')
            print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] MUST_DO_SOMETHING--CURRENT_GE_CONFIG_VERSION: {CURRENT_GE_CONFIG_VERSION} ; TYPE: {str(type(CURRENT_GE_CONFIG_VERSION))}')
            upgrade_message: str = f"""The config_version of your great_expectations.yml -- {float(ge_config_version)} -- is NOT outdated.
            Please consult the 0.14.x migration guide <ALEX -- Alex> https://docs.greatexpectations.io/en/latest/guides/how_to_guides/migrating_versions.html and
            upgrade your Great Expectations configuration to version {float(CURRENT_GE_CONFIG_VERSION)} in order to take advantage of the latest capabilities.
"""
            is_config_ok: bool = False
            context = None
            # TODO: <Alex>ALEX -- if nothing do do, then upgrade_message = None and is_config_ok = True</Alex>
            return (
                is_config_ok,
                upgrade_message,
                context,
            )
            # TODO: <Alex>ALEX</Alex>
    except (
        ge_exceptions.InvalidConfigurationYamlError,
        ge_exceptions.InvalidTopLevelConfigKeyError,
        ge_exceptions.MissingTopLevelConfigKeyError,
        ge_exceptions.InvalidConfigValueTypeError,
        ge_exceptions.UnsupportedConfigVersionError,
        ge_exceptions.DataContextError,
        ge_exceptions.PluginClassNotFoundError,
        ge_exceptions.PluginModuleNotFoundError,
        ge_exceptions.GreatExpectationsError,
    ) as err:
        print(f'\n[ALEX_TEST] [CLI_V3.DO_CONFIG_CHECK] BOTTOM-ERROR: {err} ; TYPE: {str(type(err))}')
        return False, err.message, None
