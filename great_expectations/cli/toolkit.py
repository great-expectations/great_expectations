from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

import click

from great_expectations import exceptions as gx_exceptions
from great_expectations.cli.batch_request import get_batch_request
from great_expectations.cli.cli_messages import SECTION_SEPARATOR
from great_expectations.cli.pretty_printing import cli_colorize_string, cli_message
from great_expectations.cli.upgrade_helpers import GE_UPGRADE_HELPER_VERSION_MAP
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import CURRENT_GX_CONFIG_VERSION
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.util import get_context

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.types.checkpoint_result import (
        CheckpointResult,
    )
    from great_expectations.core.batch import JSONValues
    from great_expectations.datasource import LegacyDatasource
    from great_expectations.datasource.fluent.interfaces import (
        Datasource as FluentDatasource,
    )
    from great_expectations.validator.validator import Validator


logger = logging.getLogger(__name__)

EXIT_UPGRADE_CONTINUATION_MESSAGE = (
    "\nOk, exiting now. To upgrade at a later time, use the following command: "
    "<cyan>great_expectations project upgrade</cyan>\n\nTo learn more about the upgrade "
    "process, visit "
    "<cyan>https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api"
    "</cyan>.\n"
)


def prompt_profile_to_create_a_suite(
    data_context: FileDataContext,
    expectation_suite_name: str,
) -> None:
    cli_message(
        string="""
Great Expectations will create a notebook, containing code cells that select from available columns in your dataset and
generate expectations about them to demonstrate some examples of assertions you can make about your data.

When you run this notebook, Great Expectations will store these expectations in a new Expectation Suite "{:s}" here:

  {:s}
""".format(
            expectation_suite_name,
            data_context.stores[
                data_context.expectations_store_name
            ].store_backend.get_url_for_key(
                ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ).to_tuple()
            ),
        )
    )

    confirm_proceed_or_exit()


def get_or_create_expectation_suite(  # noqa: PLR0913
    expectation_suite_name: Optional[str],
    data_context: FileDataContext,
    data_asset_name: Optional[str] = None,
    usage_event: Optional[str] = None,
    suppress_usage_message: bool = False,
    batch_request: Optional[
        Union[str, Dict[str, Union[str, int, Dict[str, Any]]]]
    ] = None,
    create_if_not_exist: bool = True,
) -> ExpectationSuite:
    if expectation_suite_name is None:
        default_expectation_suite_name: str = get_default_expectation_suite_name(
            data_asset_name=data_asset_name,
            batch_request=batch_request,
        )
        while True:
            expectation_suite_name = cast(
                str,  # click.prompt doesn't know the default type
                click.prompt(
                    "\nName the new Expectation Suite",
                    default=default_expectation_suite_name,
                ),
            )
            if (
                expectation_suite_name
                not in data_context.list_expectation_suite_names()
            ):
                break
            tell_user_suite_exists(
                data_context=data_context,
                expectation_suite_name=expectation_suite_name,
                usage_event=usage_event,
                suppress_usage_message=suppress_usage_message,
            )
    elif expectation_suite_name in data_context.list_expectation_suite_names():
        tell_user_suite_exists(
            data_context=data_context,
            expectation_suite_name=expectation_suite_name,
            usage_event=usage_event,
            suppress_usage_message=suppress_usage_message,
        )

    suite: ExpectationSuite = load_expectation_suite(
        data_context=data_context,
        expectation_suite_name=expectation_suite_name,
        usage_event=usage_event,
        suppress_usage_message=suppress_usage_message,
        create_if_not_exist=create_if_not_exist,
    )

    return suite


def get_default_expectation_suite_name(
    data_asset_name: Optional[str],
    batch_request: Optional[
        Union[str, Dict[str, Union[str, int, Dict[str, Any]]]]
    ] = None,
) -> str:
    suite_name: str
    if data_asset_name:
        suite_name = f"{data_asset_name}.warning"
    elif batch_request:
        suite_name = f"batch-{BatchRequest(**batch_request).id}"  # type: ignore[arg-type] # cannot unpack str
    else:
        suite_name = "warning"
    return suite_name


def tell_user_suite_exists(
    data_context: FileDataContext,
    expectation_suite_name: str,
    usage_event: Optional[str],
    suppress_usage_message: bool = False,
) -> None:
    exit_with_failure_message_and_stats(
        data_context=data_context,
        usage_event=usage_event,
        suppress_usage_message=suppress_usage_message,
        message=f"""<red>An expectation suite named `{expectation_suite_name}` already exists.</red>
    - If you intend to edit the suite please use `great_expectations suite edit {expectation_suite_name}`.""",
    )


def launch_jupyter_notebook(notebook_path: str) -> None:
    jupyter_command_override: Optional[str] = os.getenv("GE_JUPYTER_CMD", None)
    if jupyter_command_override:
        subprocess.call(f"{jupyter_command_override} {notebook_path}", shell=True)
    else:
        subprocess.call(["jupyter", "notebook", notebook_path])


def get_validator(
    context: FileDataContext,
    batch_request: Union[dict, BatchRequest],
    suite: Union[str, ExpectationSuite],
) -> Validator:
    assert isinstance(
        suite, (str, ExpectationSuite)
    ), "Invalid suite type (must be ExpectationSuite) or a string."

    if isinstance(batch_request, dict):
        batch_request = BatchRequest(**batch_request)

    validator: Validator
    if isinstance(suite, str):
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name=suite
        )
    else:
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite=suite
        )
    return validator


def load_expectation_suite(  # type: ignore[return] # sys.exit if no suite
    data_context: FileDataContext,
    expectation_suite_name: str,
    usage_event: Optional[str],
    suppress_usage_message: bool = False,
    create_if_not_exist: bool = True,
) -> ExpectationSuite:
    """
    Load an expectation suite from a given context.

    Handles a suite name with or without `.json`
    :param data_context:
    :param expectation_suite_name:
    :param usage_event:
    :param suppress_usage_message:
    :param create_if_not_exist:
    """
    if expectation_suite_name.endswith(".json"):
        expectation_suite_name = expectation_suite_name[:-5]

    suite: Optional[ExpectationSuite] = None
    try:
        suite = data_context.get_expectation_suite(
            expectation_suite_name=expectation_suite_name
        )
    except gx_exceptions.DataContextError:
        if create_if_not_exist:
            suite = data_context.add_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )

    if suite:
        return suite
    exit_with_failure_message_and_stats(
        data_context=data_context,
        usage_event=usage_event,
        suppress_usage_message=suppress_usage_message,
        message=f"<red>Could not find a suite named `{expectation_suite_name}`.</red> Please check "
        "the name by running `great_expectations suite list` and try again.",
    )


def exit_with_failure_message_and_stats(
    data_context: FileDataContext,
    usage_event: Optional[str],
    suppress_usage_message: bool = False,
    message: Optional[str] = None,
) -> None:
    if message:
        cli_message(string=message)
    if not suppress_usage_message:
        send_usage_message(
            data_context=data_context,
            event=usage_event,
            success=False,
        )
    sys.exit(1)


def delete_checkpoint(
    context: FileDataContext,
    checkpoint_name: str,
    usage_event: str,
    assume_yes: bool,
) -> None:
    """Delete a Checkpoint or raise helpful errors."""
    validate_checkpoint(
        context=context,
        checkpoint_name=checkpoint_name,
        usage_event=usage_event,
    )
    confirm_prompt: str = f"""\nAre you sure you want to delete the Checkpoint "{checkpoint_name}" (this action is irreversible)?"
"""
    continuation_message: str = (
        f'The Checkpoint "{checkpoint_name}" was not deleted.  Exiting now.'
    )
    if not assume_yes:
        confirm_proceed_or_exit(
            confirm_prompt=confirm_prompt,
            continuation_message=continuation_message,
            data_context=context,
            usage_stats_event=usage_event,
        )
    context.delete_checkpoint(name=checkpoint_name)


def run_checkpoint(  # type: ignore[return] # sys.exit if no result
    context: FileDataContext,
    checkpoint_name: str,
    usage_event: str,
) -> CheckpointResult:
    """Run a Checkpoint or raise helpful errors."""
    failure_message: str = "Exception occurred while running Checkpoint."
    validate_checkpoint(
        context=context,
        checkpoint_name=checkpoint_name,
        usage_event=usage_event,
        failure_message=failure_message,
    )
    try:
        result: CheckpointResult = context.run_checkpoint(
            checkpoint_name=checkpoint_name
        )
        return result
    except gx_exceptions.CheckpointError as e:
        cli_message(string=failure_message)
        exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event,
            message=f"<red>{e}.</red>",
        )


def validate_checkpoint(
    context: FileDataContext,
    checkpoint_name: str,
    usage_event: str,
    failure_message: Optional[str] = None,
) -> None:
    try:
        _ = load_checkpoint(
            context=context, checkpoint_name=checkpoint_name, usage_event=usage_event
        )
    except gx_exceptions.CheckpointError as e:
        if failure_message:
            cli_message(string=failure_message)
        exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event,
            message=f"<red>{e}.</red>",
        )


def load_checkpoint(  # type: ignore[return] # sys.exit if no checkpoint
    context: FileDataContext,
    checkpoint_name: str,
    usage_event: str,
) -> Checkpoint:
    """Load a Checkpoint or raise helpful errors."""
    try:
        checkpoint: Checkpoint = context.get_checkpoint(name=checkpoint_name)
        return checkpoint
    except (
        gx_exceptions.CheckpointNotFoundError,
        gx_exceptions.InvalidCheckpointConfigError,
    ):
        exit_with_failure_message_and_stats(
            data_context=context,
            usage_event=usage_event,
            message=f"""\
<red>Could not find Checkpoint `{checkpoint_name}` (or its configuration is invalid).</red> Try running:
  - `<green>great_expectations checkpoint list</green>` to verify your Checkpoint exists
  - `<green>great_expectations checkpoint new</green>` to configure a new Checkpoint""",
        )


def select_datasource(
    context: FileDataContext, datasource_name: Optional[str] = None
) -> Union[BaseDatasource, LegacyDatasource, FluentDatasource, None]:
    """Select a datasource interactively."""
    # TODO consolidate all the myriad CLI tests into this
    data_source: Union[BaseDatasource, LegacyDatasource, FluentDatasource, None] = None

    if datasource_name is None:
        # exclude fluent datasources from options
        block_style_datasources = [
            x
            for x in context.datasources.values()
            if x.name not in context.fluent_datasources
        ]
        data_sources: List[BaseDatasource] = cast(
            List[BaseDatasource],
            list(
                sorted(block_style_datasources, key=lambda x: (len(x.name), x.name)),
            ),
        )
        if len(data_sources) == 0:
            cli_message(
                string="<red>No datasources found in the context. To add a datasource, run `great_expectations datasource new`</red>"
            )
        elif len(data_sources) == 1:
            datasource_name = data_sources[0].name
        else:
            choices: str = "\n".join(
                [
                    f"    {i}. {data_source.name}"
                    for i, data_source in enumerate(data_sources, 1)
                ]
            )
            option_selection: str = click.prompt(
                f"Select a datasource\n{choices}\n",
                type=click.Choice(
                    [str(i) for i, data_source in enumerate(data_sources, 1)]
                ),
                show_choices=False,
            )
            datasource_name = data_sources[int(option_selection) - 1].name

    if datasource_name is not None:
        data_source = context.get_datasource(datasource_name=datasource_name)

    return data_source


def load_data_context_with_error_handling(  # noqa: PLR0912
    directory: Optional[str], from_cli_upgrade_command: bool = False
) -> Optional[FileDataContext]:
    """Return a DataContext with good error handling and exit codes."""
    context: Optional[FileDataContext]
    ge_config_version: float
    try:
        if directory:
            # Without this check, FileDataContext will possibly scaffold a project structure.
            # As we want CLI users to follow the `init` workflow, we should exit early if we can't find a context YAML.
            if not FileDataContext._find_context_yml_file(directory):
                raise gx_exceptions.ConfigNotFoundError()
        else:
            directory = FileDataContext.find_context_root_dir()

        context = get_context(context_root_dir=directory)
        ge_config_version = context.get_config().config_version  # type: ignore[union-attr] # could be dict, str

        if from_cli_upgrade_command:
            if ge_config_version < CURRENT_GX_CONFIG_VERSION:
                context = upgrade_project_one_or_multiple_versions_increment(
                    directory=directory,
                    context=context,
                    ge_config_version=ge_config_version,
                    from_cli_upgrade_command=from_cli_upgrade_command,
                )
            elif ge_config_version > CURRENT_GX_CONFIG_VERSION:
                raise gx_exceptions.UnsupportedConfigVersionError(
                    f"""Invalid config version ({ge_config_version}).\n    The maximum valid version is \
{CURRENT_GX_CONFIG_VERSION}.
"""
                )
            else:
                context = upgrade_project_zero_versions_increment(
                    directory=directory,
                    context=context,
                    ge_config_version=ge_config_version,
                    from_cli_upgrade_command=from_cli_upgrade_command,
                )

        return context
    except gx_exceptions.UnsupportedConfigVersionError as err:
        directory = directory or FileDataContext.find_context_root_dir()
        ge_config_version = FileDataContext.get_ge_config_version(  # type: ignore[assignment] # could be none
            context_root_dir=directory
        )
        context = upgrade_project_strictly_multiple_versions_increment(
            directory=directory,
            ge_config_version=ge_config_version,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        if context:
            return context
        else:
            cli_message(string=f"<red>{err.message}</red>")
            sys.exit(1)
    except (
        gx_exceptions.ConfigNotFoundError,
        gx_exceptions.InvalidConfigError,
    ) as err:
        cli_message(string=f"<red>{err.message}</red>")
        sys.exit(1)
    except gx_exceptions.PluginModuleNotFoundError as err:
        cli_message(string=err.cli_colored_message)
        sys.exit(1)
    except gx_exceptions.PluginClassNotFoundError as err:
        cli_message(string=err.cli_colored_message)
        sys.exit(1)
    except gx_exceptions.InvalidConfigurationYamlError as err:
        cli_message(string=f"<red>{str(err)}</red>")
        sys.exit(1)


def upgrade_project_strictly_multiple_versions_increment(
    directory: str, ge_config_version: float, from_cli_upgrade_command: bool = False
) -> Optional[FileDataContext]:
    upgrade_helper_class = (
        GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
        if ge_config_version
        else None
    )
    context: Optional[FileDataContext]
    if upgrade_helper_class and int(ge_config_version) < CURRENT_GX_CONFIG_VERSION:
        upgrade_project(
            context_root_dir=directory,
            ge_config_version=ge_config_version,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        context = get_context(context_root_dir=directory)
        # noinspection PyBroadException
        try:
            send_usage_message(
                data_context=context,
                event=UsageStatsEvents.CLI_PROJECT_UPGRADE_END,
                success=True,
            )
        except Exception:
            # Don't fail for usage stats
            pass
    else:
        context = None

    return context


def upgrade_project(
    context_root_dir: str,
    ge_config_version: float,
    from_cli_upgrade_command: bool = False,
) -> None:
    if from_cli_upgrade_command:
        message = (
            f"<red>\nYour project appears to have an out-of-date config version ({ge_config_version}) - "
            f"the version "
            f"number must be at least {CURRENT_GX_CONFIG_VERSION}.</red>"
        )
    else:
        message = (
            f"<red>\nYour project appears to have an out-of-date config version ({ge_config_version}) - "
            f"the version "
            f"number must be at least {CURRENT_GX_CONFIG_VERSION}.\nIn order to proceed, "
            f"your project must be upgraded.</red>"
        )

    cli_message(string=message)
    upgrade_prompt = (
        "\nWould you like to run the Upgrade Helper to bring your project up-to-date?"
    )
    # This loading of DataContext is optional and just to track if someone exits here.
    # noinspection PyBroadException
    try:
        data_context = get_context(context_root_dir=context_root_dir)
    except Exception:
        # Do not raise error for usage stats
        data_context = None

    confirm_proceed_or_exit(
        confirm_prompt=upgrade_prompt,
        continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
        data_context=data_context,
        usage_stats_event=UsageStatsEvents.CLI_PROJECT_UPGRADE_END,
    )
    cli_message(string=SECTION_SEPARATOR)

    # use loop in case multiple upgrades need to take place
    while int(ge_config_version) < CURRENT_GX_CONFIG_VERSION:
        (
            increment_version,
            exception_occurred,
        ) = upgrade_project_up_to_one_version_increment(
            context_root_dir=context_root_dir,
            ge_config_version=ge_config_version,
            continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
            update_version=True,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        if exception_occurred or not increment_version:
            break

        ge_config_version += 1.0

    cli_message(string=SECTION_SEPARATOR)
    upgrade_success_message = "<green>Upgrade complete. Exiting...</green>\n"
    upgrade_incomplete_message = f"""\
<red>The Upgrade Helper was unable to perform a complete project upgrade. Next steps:</red>

    - Please perform any manual steps outlined in the Upgrade Overview and/or Upgrade Report above
    - When complete, increment the config_version key in your <cyan>great_expectations.yml</cyan> to <cyan>{ge_config_version + 1.0}</cyan>\n
To learn more about the upgrade process, visit \
<cyan>https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api</cyan>
"""

    if int(ge_config_version) < CURRENT_GX_CONFIG_VERSION:
        cli_message(string=upgrade_incomplete_message)
    else:
        cli_message(upgrade_success_message)

    # noinspection PyBroadException
    try:
        data_context = get_context(context_root_dir=context_root_dir)
        send_usage_message(
            data_context=data_context,
            event=UsageStatsEvents.CLI_PROJECT_UPGRADE_END,
            success=True,
        )
    except Exception:
        # Do not raise error for usage stats
        pass

    sys.exit(0)


def upgrade_project_one_or_multiple_versions_increment(  # noqa: PLR0912
    directory: str,
    context: FileDataContext,
    ge_config_version: float,
    from_cli_upgrade_command: bool = False,
) -> Optional[FileDataContext]:
    # noinspection PyBroadException
    try:
        send_usage_message(
            data_context=context,
            event=UsageStatsEvents.CLI_PROJECT_UPGRADE_BEGIN,
            success=True,
        )
    except Exception:
        # Don't fail for usage stats
        pass

    upgrade_successful: bool = False

    if (CURRENT_GX_CONFIG_VERSION - int(ge_config_version)) == 1:
        (
            increment_version,
            exception_occurred,
        ) = upgrade_project_up_to_one_version_increment(
            context_root_dir=directory,
            ge_config_version=ge_config_version,
            continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
            update_version=True,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        if not exception_occurred and increment_version:
            upgrade_successful = True
    else:
        upgrade_project(
            context_root_dir=directory,
            ge_config_version=ge_config_version,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        upgrade_successful = True

    if upgrade_successful:
        upgrade_helper_class = (
            GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
            if ge_config_version
            else None
        )
        if upgrade_helper_class:
            upgrade_helper = upgrade_helper_class(
                context_root_dir=directory, update_version=False
            )
        else:
            error_message: str = f"The upgrade utility for version {ge_config_version} could not be found."
            cli_message(string=f"<red>{error_message}</red>")
            sys.exit(1)

        manual_steps_required = upgrade_helper.manual_steps_required()
        if manual_steps_required:
            upgrade_message = "Your project requires manual upgrade steps in order to be up-to-date.\n"
            cli_message(f"<yellow>{upgrade_message}</yellow>")
        else:
            upgrade_message = (
                "Your project is up-to-date - no further upgrade is necessary.\n"
            )
            cli_message(f"<green>{upgrade_message}</green>")

        context = get_context(context_root_dir=directory)

        # noinspection PyBroadException
        try:
            send_usage_message(
                data_context=context,
                event=UsageStatsEvents.CLI_PROJECT_UPGRADE_END,
                success=True,
            )
        except Exception:
            # Don't fail for usage stats
            pass
    else:
        return None

    return context


def upgrade_project_zero_versions_increment(
    directory: str,
    context: FileDataContext,
    ge_config_version: float,
    from_cli_upgrade_command: bool = False,
) -> Optional[FileDataContext]:
    upgrade_helper_class = (
        GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
        if ge_config_version
        else None
    )
    if upgrade_helper_class:
        upgrade_helper = upgrade_helper_class(
            context_root_dir=directory, update_version=False
        )
    else:
        error_message: str = (
            f"The upgrade utility for version {ge_config_version} could not be found."
        )
        cli_message(string=f"<red>{error_message}</red>")
        sys.exit(1)

    manual_steps_required = upgrade_helper.manual_steps_required()

    if manual_steps_required:
        # noinspection PyBroadException
        try:
            send_usage_message(
                data_context=context,
                event=UsageStatsEvents.CLI_PROJECT_UPGRADE_BEGIN,
                success=True,
            )
        except Exception:
            # Don't fail for usage stats
            pass

    (
        increment_version,
        exception_occurred,
    ) = upgrade_project_up_to_one_version_increment(
        context_root_dir=directory,
        ge_config_version=ge_config_version,
        continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
        update_version=False,
        from_cli_upgrade_command=from_cli_upgrade_command,
    )
    if exception_occurred or increment_version:
        return None

    if manual_steps_required:
        upgrade_message = (
            "Your project requires manual upgrade steps in order to be up-to-date.\n"
        )
        cli_message(f"<yellow>{upgrade_message}</yellow>")
    else:
        upgrade_message = (
            "Your project is up-to-date - no further upgrade is necessary.\n"
        )
        cli_message(f"<green>{upgrade_message}</green>")

    context = get_context(context_root_dir=directory)

    # noinspection PyBroadException
    try:
        send_usage_message(
            data_context=context,
            event=UsageStatsEvents.CLI_PROJECT_UPGRADE_END,
            success=True,
        )
    except Exception:
        # Don't fail for usage stats
        pass

    return context


def upgrade_project_up_to_one_version_increment(
    context_root_dir: str,
    ge_config_version: float,
    continuation_message: str,
    update_version: bool,
    from_cli_upgrade_command: bool = False,
) -> Tuple[bool, bool]:  # Returns increment_version, exception_occurred:
    upgrade_helper_class = GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
    if not upgrade_helper_class:
        return False, False

    # set version temporarily to CURRENT_GX_CONFIG_VERSION to get functional DataContext
    FileDataContext.set_ge_config_version(
        config_version=CURRENT_GX_CONFIG_VERSION,
        context_root_dir=context_root_dir,
    )

    upgrade_helper = upgrade_helper_class(
        context_root_dir=context_root_dir, update_version=update_version
    )

    manual_steps_required = upgrade_helper.manual_steps_required()

    if not (update_version or manual_steps_required):
        return False, False

    upgrade_overview, confirmation_required = upgrade_helper.get_upgrade_overview()

    if from_cli_upgrade_command and confirmation_required:
        upgrade_confirmed = confirm_proceed_or_exit(
            confirm_prompt=upgrade_overview,
            continuation_message=continuation_message,
            exit_on_no=False,
        )
    else:
        upgrade_confirmed = True

    if upgrade_confirmed:
        if confirmation_required:
            cli_message(string="\nUpgrading project...")
            cli_message(string=SECTION_SEPARATOR)

        # run upgrade and get report of what was done, if version number should be incremented
        (
            upgrade_report,
            increment_version,
            exception_occurred,
        ) = upgrade_helper.upgrade_project()
        # display report to user
        cli_message(string=upgrade_report)
        if exception_occurred:
            # restore version number to current number
            FileDataContext.set_ge_config_version(
                ge_config_version, context_root_dir, validate_config_version=False
            )
            # display report to user
            return False, True
        # set config version to target version
        if increment_version:
            FileDataContext.set_ge_config_version(
                int(ge_config_version) + 1,
                context_root_dir,
                validate_config_version=False,
            )
            return True, False
        # restore version number to current number
        FileDataContext.set_ge_config_version(
            ge_config_version, context_root_dir, validate_config_version=False
        )
        return False, False

    # restore version number to current number
    FileDataContext.set_ge_config_version(
        ge_config_version, context_root_dir, validate_config_version=False
    )
    cli_message(string=continuation_message)
    sys.exit(0)


def confirm_proceed_or_exit(  # noqa: PLR0913
    confirm_prompt: str = "Would you like to proceed?",
    continuation_message: str = "Ok, exiting now. You can always read more at https://docs.greatexpectations.io/ !",
    exit_on_no: bool = True,
    exit_code: int = 0,
    data_context: Optional[FileDataContext] = None,
    usage_stats_event: Optional[str] = None,
) -> bool:
    """
    Every CLI command that starts a potentially lengthy (>1 sec) computation
    or modifies some resources (e.g., edits the config file, adds objects
    to the stores) must follow this pattern:
    1. Explain which resources will be created/modified/deleted
    2. Use this method to ask for user's confirmation

    The goal of this standardization is for the users to expect consistency -
    if you saw one command, you know what to expect from all others.

    If the user does not confirm, the program should exit. The purpose of the exit_on_no parameter is to provide
    the option to perform cleanup actions before exiting outside of the function.
    """
    confirm_prompt_colorized = cli_colorize_string(confirm_prompt)
    continuation_message_colorized = cli_colorize_string(continuation_message)
    if not click.confirm(confirm_prompt_colorized, default=True):
        if exit_on_no:
            cli_message(string=continuation_message_colorized)
            cli_message(string=continuation_message_colorized)
            if (usage_stats_event is not None) and (data_context is not None):
                # noinspection PyBroadException
                try:
                    send_usage_message(
                        data_context=data_context,
                        event=usage_stats_event,
                        event_payload={"cancelled": True},
                        success=True,
                    )
                except Exception as e:
                    # Don't fail on usage stats
                    logger.debug(f"Something went wrong when sending usage stats: {e}")
                    pass
            sys.exit(exit_code)
        else:
            return False
    return True


def parse_cli_config_file_location(config_file_location: str) -> dict:
    """
    Parse CLI yaml config file or directory location into directory and filename.
    Uses pathlib to handle windows paths.
    Args:
        config_file_location: string of config_file_location

    Returns:
        {
            "directory": "directory/where/config/file/is/located",
            "filename": "great_expectations.yml"
        }
    """

    if config_file_location is not None and config_file_location != "":  # noqa: PLC1901
        config_file_location_path = Path(config_file_location)

        # If the file or directory exists, treat it appropriately
        # This handles files without extensions
        filename: Optional[str]
        directory: Optional[str]
        if config_file_location_path.is_file():
            filename = rf"{str(config_file_location_path.name)}"
            directory = rf"{str(config_file_location_path.parent)}"
        elif config_file_location_path.is_dir():
            filename = None
            directory = config_file_location

        else:
            raise gx_exceptions.ConfigNotFoundError()

    else:
        # Return None if config_file_location is empty rather than default output of ""
        directory = None
        filename = None

    return {"directory": directory, "filename": filename}


def is_cloud_file_url(file_path: str) -> bool:
    """Check for commonly used cloud urls."""
    sanitized = file_path.strip()
    if sanitized[0:7] == "file://":
        return False
    if (
        sanitized[0:5] in ["s3://", "gs://"]
        or sanitized[0:6] == "ftp://"
        or sanitized[0:7] in ["http://", "wasb://"]
        or sanitized[0:8] == "https://"
    ):
        return True
    return False


def get_relative_path_from_config_file_to_base_path(
    context_root_directory: str, data_path: str
) -> str:
    """
    This function determines the relative path from a given data path relative
    to the great_expectations.yml file independent of the current working
    directory.

    This allows a user to use the CLI from any directory, type a relative path
    from their current working directory and have the correct relative path be
    put in the great_expectations.yml file.
    """
    data_from_working_dir = os.path.relpath(data_path)
    context_dir_from_working_dir = os.path.relpath(context_root_directory)
    return os.path.relpath(data_from_working_dir, context_dir_from_working_dir)


def load_json_file_into_dict(
    filepath: str,
    data_context: FileDataContext,
    usage_event: Optional[str] = None,
) -> Optional[Dict[str, Union[str, int, Dict[str, Any]]]]:
    suppress_usage_message: bool = (usage_event is None) or (data_context is None)

    error_message: str

    if not filepath:
        error_message = "The path to a JSON file was not specified."
        exit_with_failure_message_and_stats(
            data_context=data_context,
            usage_event=usage_event,
            suppress_usage_message=suppress_usage_message,
            message=f"<red>{error_message}</red>",
        )

    if not filepath.endswith(".json"):
        error_message = f'The JSON file path "{filepath}" does not have the ".json" extension in the file name.'
        exit_with_failure_message_and_stats(
            data_context=data_context,
            usage_event=usage_event,
            suppress_usage_message=suppress_usage_message,
            message=f"<red>{error_message}</red>",
        )

    contents: Optional[str] = None
    try:
        with open(filepath) as json_file:
            contents = json_file.read()
    except FileNotFoundError:
        error_message = f'The JSON file with the path "{filepath}" could not be found.'
        exit_with_failure_message_and_stats(
            data_context=data_context,
            usage_event=usage_event,
            suppress_usage_message=suppress_usage_message,
            message=f"<red>{error_message}</red>",
        )

    batch_request: Optional[Dict[str, Union[str, int, Dict[str, Any]]]] = None
    if contents:
        try:
            batch_request = json.loads(contents)
        except JSONDecodeError as jde:
            error_message = f"""Error "{jde}" occurred while attempting to load the JSON file with the path
"{filepath}" into dictionary.
"""
            exit_with_failure_message_and_stats(
                data_context=data_context,
                usage_event=usage_event,
                suppress_usage_message=suppress_usage_message,
                message=f"<red>{error_message}</red>",
            )
    else:
        error_message = f'The JSON file path "{filepath}" is empty.'
        exit_with_failure_message_and_stats(
            data_context=data_context,
            usage_event=usage_event,
            suppress_usage_message=suppress_usage_message,
            message=f"<red>{error_message}</red>",
        )

    return batch_request


def get_batch_request_from_citations(
    expectation_suite: Optional[ExpectationSuite] = None,
) -> Optional[Union[str, Dict[str, Union[str, int, Dict[str, Any]]]]]:
    batch_request_from_citation: Optional[
        Union[str, Dict[str, Union[str, int, Dict[str, Any]]]]
    ] = None

    if expectation_suite is not None:
        citations: List[Dict[str, Any]] = expectation_suite.get_citations(
            require_batch_request=True
        )
        if citations:
            citation: Dict[str, Any] = citations[-1]
            batch_request_from_citation = citation.get("batch_request")

    return batch_request_from_citation


def add_citation_with_batch_request(
    data_context: FileDataContext,
    expectation_suite: ExpectationSuite,
    batch_request: Optional[Dict[str, Union[str, int, Dict[str, Any]]]] = None,
) -> None:
    if (
        expectation_suite is not None
        and batch_request
        and isinstance(batch_request, dict)
        and BatchRequest(**batch_request)  # type: ignore[arg-type] # values union
    ):
        expectation_suite.add_citation(
            comment="Created suite added via CLI",
            batch_request=batch_request,  # type: ignore[arg-type] # values union
        )
        data_context.update_expectation_suite(expectation_suite=expectation_suite)


def get_batch_request_from_json_file(
    batch_request_json_file_path: str,
    data_context: FileDataContext,
    usage_event: Optional[str] = None,
    suppress_usage_message: bool = False,
) -> dict[str, JSONValues]:
    batch_request_or_error_message: Optional[
        str | dict[str, str | int | dict[str, Any]]
    ] = load_json_file_into_dict(
        filepath=batch_request_json_file_path,
        data_context=data_context,
        usage_event=usage_event,
    )
    try:
        if not batch_request_or_error_message or isinstance(
            batch_request_or_error_message, str
        ):
            raise TypeError(batch_request_or_error_message)
        else:
            batch_request_json_dict: dict[str, JSONValues] = BatchRequest(
                **batch_request_or_error_message  # type: ignore[arg-type]
            ).to_json_dict()
    except TypeError as e:
        cli_message(
            string="<red>Please check that your batch_request is valid and is able to load a batch.</red>"
        )
        cli_message(string=f"<red>{e}</red>")
        if not suppress_usage_message:
            send_usage_message(
                data_context=data_context,
                event=usage_event,
                success=False,
            )
        sys.exit(1)

    return batch_request_json_dict


def get_batch_request_using_datasource_name(
    data_context: FileDataContext,
    datasource_name: Optional[str] = None,
    usage_event: Optional[str] = None,
    suppress_usage_message: bool = False,
    additional_batch_request_args: Optional[
        Dict[str, Union[str, int, Dict[str, Any]]]
    ] = None,
) -> Optional[Dict[str, Union[str, int, Dict[str, Any]]]]:
    cli_message(
        string="\nA batch of data is required to edit the suite - let's help you to specify it.\n"
    )

    datasource = select_datasource(
        context=data_context, datasource_name=datasource_name
    )

    if not datasource:
        cli_message(string="<red>No datasources found in the context.</red>")
        if not suppress_usage_message:
            send_usage_message(
                data_context=data_context,
                event=usage_event,
                success=False,
            )
        sys.exit(1)

    batch_request: Dict[str, Union[str, int, Dict[str, Any]]] = get_batch_request(
        datasource=datasource,  # type: ignore[arg-type] # could be LegacyDatasource
        additional_batch_request_args=additional_batch_request_args,
    )
    if "data_asset_name" not in batch_request:
        # Some datasources don't always provide this (`RuntimeDataConnector`)
        batch_request["data_asset_name"] = "default_data_asset_name"

    return batch_request
