import datetime
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union, cast

import click
from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO

from great_expectations import exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.cli.batch_request import (
    get_batch_request,
    standardize_batch_request_display_ordering,
)
from great_expectations.cli.cli_messages import SECTION_SEPARATOR
from great_expectations.cli.pretty_printing import cli_colorize_string, cli_message
from great_expectations.cli.upgrade_helpers import GE_UPGRADE_HELPER_VERSION_MAP
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.usage_statistics import (
    send_usage_message as send_usage_stats_message,
)
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.base import CURRENT_GE_CONFIG_VERSION
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.profile import BasicSuiteBuilderProfiler
from great_expectations.validator.validator import Validator

try:
    from termcolor import colored
except ImportError:
    pass


EXIT_UPGRADE_CONTINUATION_MESSAGE = (
    "\nOk, exiting now. To upgrade at a later time, use the following command: "
    "<cyan>great_expectations project upgrade</cyan>\n\nTo learn more about the upgrade "
    "process, visit "
    "<cyan>https://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html"
    "</cyan>.\n"
)


class MyYAML(YAML):
    # copied from https://yaml.readthedocs.io/en/latest/example.html#output-of-dump-as-a-string
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()


yaml = MyYAML()  # or typ='safe'/'unsafe' etc

yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


def create_expectation_suite(
    context: DataContext,
    datasource_name: Optional[str] = None,
    data_connector_name: Optional[str] = None,
    data_asset_name: Optional[str] = None,
    batch_request: Optional[dict] = None,
    expectation_suite_name: Optional[str] = None,
    no_dataset: Optional[bool] = False,
    scaffold: Optional[bool] = False,
    additional_batch_request_args: Optional[dict] = None,
    # TODO: <Alex>ALEX -- Is this needed?</Alex>
    flag_build_docs: Optional[bool] = True,
    # TODO: <Alex>ALEX -- Is this needed?</Alex>
    profiler_configuration: Optional[str] = "demo",
) -> Tuple[str, Optional[dict]]:
    """
    Create a new expectation suite.

    :return: a tuple: (suite name, profiling_results)
    """
    if not no_dataset:
        data_source: Optional[BaseDatasource] = select_datasource(
            context=context, datasource_name=datasource_name
        )

        if data_source is None:
            # select_datasource takes care of displaying an error message, so all is left here is to exit.
            sys.exit(1)

        datasource_name = data_source.name

        if (
            data_connector_name is None
            or data_asset_name is None
            or batch_request is None
        ):
            (
                datasource_name,
                data_connector_name,
                data_asset_name,
                batch_request,
            ) = get_batch_request(
                context=context,
                datasource_name=datasource_name,
                data_connector_name=data_connector_name,
                additional_batch_request_args=additional_batch_request_args,
            )
            # In this case, we have "consumed" the additional_batch_request_args
            additional_batch_request_args = {}

    if batch_request:
        batch_request = standardize_batch_request_display_ordering(
            batch_request=batch_request
        )

    if expectation_suite_name is None:
        default_expectation_suite_name: str = _get_default_expectation_suite_name(
            data_asset_name=data_asset_name,
            batch_request=batch_request,
        )
        while True:
            expectation_suite_name = click.prompt(
                "\nName the new Expectation Suite",
                default=default_expectation_suite_name,
            )
            if expectation_suite_name not in context.list_expectation_suite_names():
                break
            tell_user_suite_exists(expectation_suite_name)
    elif expectation_suite_name in context.list_expectation_suite_names():
        tell_user_suite_exists(expectation_suite_name)
        sys.exit(1)

    create_empty_suite(
        context=context,
        expectation_suite_name=expectation_suite_name,
        no_dataset=no_dataset,
        batch_request=batch_request,
    )
    return expectation_suite_name, None

    # TODO: <Alex>ALEX -- This scaffold.</Alex>
    # TODO: <Alex>ALEX -- How will this be in V3?</Alex>
    # profiling_results: dict = _profile_to_create_a_suite(
    #     additional_batch_request_args,
    #     batch_request,
    #     data_connector_name,
    #     context,
    #     datasource_name,
    #     expectation_suite_name,
    #     data_asset_name,
    #     profiler_configuration,
    # )
    #
    # # TODO: <Alex>ALEX -- When is this used?</Alex>
    # if flag_build_docs:
    #     build_docs(context=context, view=False)
    #     if open_docs:  # TODO: <Alex>ALEX -- "open_docs" is always False; when is next line called?</Alex>
    #         attempt_to_open_validation_results_in_data_docs(context=context, profiling_results=profiling_results)
    #
    # return expectation_suite_name, profiling_results


# TODO: <Alex>ALEX - Update for V3</Alex>
def _profile_to_create_a_suite(
    additional_batch_kwargs,
    batch_request,
    batch_kwargs_generator_name,
    context,
    datasource_name,
    expectation_suite_name,
    data_asset_name,
    profiler_configuration,
):

    cli_message(
        """
Great Expectations will choose a couple of columns and generate expectations about them
to demonstrate some examples of assertions you can make about your data.

Great Expectations will store these expectations in a new Expectation Suite '{:s}' here:

  {:s}
""".format(
            expectation_suite_name,
            context.stores[
                context.expectations_store_name
            ].store_backend.get_url_for_key(
                ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ).to_tuple()
            ),
        )
    )

    confirm_proceed_or_exit()

    # TODO this may not apply
    cli_message("\nGenerating example Expectation Suite...")
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    profiling_results = context.profile_data_asset(
        datasource_name,
        batch_kwargs_generator_name=batch_kwargs_generator_name,
        data_asset_name=data_asset_name,
        batch_kwargs=batch_request,
        profiler=BasicSuiteBuilderProfiler,
        profiler_configuration=profiler_configuration,
        expectation_suite_name=expectation_suite_name,
        run_id=run_id,
        additional_batch_kwargs=additional_batch_kwargs,
    )
    if not profiling_results["success"]:
        _raise_profiling_errors(profiling_results)

    cli_message("\nDone generating example Expectation Suite")
    return profiling_results


def _raise_profiling_errors(profiling_results):
    if (
        profiling_results["error"]["code"]
        == DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND
    ):
        raise ge_exceptions.DataContextError(
            """Some of the data assets you specified were not found: {:s}
            """.format(
                ",".join(profiling_results["error"]["not_found_data_assets"])
            )
        )
    raise ge_exceptions.DataContextError(
        "Unknown profiling error code: " + profiling_results["error"]["code"]
    )


def attempt_to_open_validation_results_in_data_docs(context, profiling_results):
    try:
        # TODO this is really brittle and not covered in tests
        validation_result = profiling_results["results"][0][1]
        validation_result_identifier = ValidationResultIdentifier.from_object(
            validation_result
        )

        context.open_data_docs(resource_identifier=validation_result_identifier)
    except (KeyError, IndexError):
        context.open_data_docs()


def _get_default_expectation_suite_name(
    data_asset_name: str,
    batch_request: Optional[Union[str, Dict[str, Union[str, Dict[str, Any]]]]] = None,
) -> str:
    suite_name: str
    if data_asset_name:
        suite_name = f"{data_asset_name}.warning"
    elif batch_request:
        suite_name = f"{data_asset_name}.{BatchRequest(**batch_request).id}"
    else:
        suite_name = "warning"
    return suite_name


def tell_user_suite_exists(suite_name: str) -> None:
    cli_message(
        f"""<red>An expectation suite named `{suite_name}` already exists.</red>
  - If you intend to edit the suite please use `great_expectations suite edit {suite_name}`."""
    )


def create_empty_suite(
    context: DataContext,
    expectation_suite_name: str,
    no_dataset: bool,
    batch_request: dict,
) -> None:
    cli_message(
        """
Great Expectations will create a new Expectation Suite '{:s}' and store it here:

  {:s}
""".format(
            expectation_suite_name,
            context.expectations_store.store_backend.get_url_for_key(
                ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ).to_tuple()
            ),
        )
    )
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    suite.add_citation(
        comment="New suite added via CLI",
        no_dataset=no_dataset,
        batch_request=batch_request,
    )
    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=expectation_suite_name
    )


def launch_jupyter_notebook(notebook_path: str) -> None:
    jupyter_command_override = os.getenv("GE_JUPYTER_CMD", None)
    if jupyter_command_override:
        subprocess.call(f"{jupyter_command_override} {notebook_path}", shell=True)
    else:
        subprocess.call(["jupyter", "notebook", notebook_path])


def get_validator(
    context: DataContext,
    batch_request: Union[dict, BatchRequest],
    suite: Union[str, ExpectationSuite],
) -> Validator:
    assert isinstance(
        suite, (str, ExpectationSuite)
    ), "Invalid suite type (must be ExpectationSuite or a string."

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


def load_expectation_suite(
    # TODO consolidate all the myriad CLI tests into this
    context: DataContext,
    suite_name: str,
    usage_event: str,
) -> ExpectationSuite:
    """
    Load an expectation suite from a given context.

    Handles a suite name with or without `.json`
    :param usage_event:
    """
    if suite_name.endswith(".json"):
        suite_name = suite_name[:-5]
    try:
        suite: ExpectationSuite = context.get_expectation_suite(suite_name)
        return suite
    except ge_exceptions.DataContextError as e:
        exit_with_failure_message_and_stats(
            context=context,
            usage_event=usage_event,
            message=f"<red>Could not find a suite named `{suite_name}`.</red> Please check "
            "the name by running `great_expectations suite list` and try again.",
        )


def exit_with_failure_message_and_stats(
    context: DataContext, usage_event: str, message: str
) -> None:
    cli_message(message)
    send_usage_message(context, event=usage_event, success=False)
    sys.exit(1)


def delete_checkpoint(
    context: DataContext,
    checkpoint_name: str,
    usage_event: str,
    assume_yes: bool,
):
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
        )
    context.delete_checkpoint(name=checkpoint_name)


def run_checkpoint(
    context: DataContext,
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
    except ge_exceptions.CheckpointError as e:
        cli_message(string=failure_message)
        exit_with_failure_message_and_stats(context, usage_event, f"<red>{e}.</red>")


def validate_checkpoint(
    context: DataContext,
    checkpoint_name: str,
    usage_event: str,
    failure_message: Optional[str] = None,
):
    try:
        # noinspection PyUnusedLocal
        checkpoint: Union[Checkpoint, LegacyCheckpoint] = load_checkpoint(
            context=context, checkpoint_name=checkpoint_name, usage_event=usage_event
        )
    except ge_exceptions.CheckpointError as e:
        if failure_message:
            cli_message(string=failure_message)
        exit_with_failure_message_and_stats(context, usage_event, f"<red>{e}</red>")


def load_checkpoint(
    context: DataContext,
    checkpoint_name: str,
    usage_event: str,
) -> Union[Checkpoint, LegacyCheckpoint]:
    """Load a Checkpoint or raise helpful errors."""
    try:
        checkpoint: Union[Checkpoint, LegacyCheckpoint] = context.get_checkpoint(
            name=checkpoint_name
        )
        return checkpoint
    except (
        ge_exceptions.CheckpointNotFoundError,
        ge_exceptions.InvalidCheckpointConfigError,
    ):
        exit_with_failure_message_and_stats(
            context,
            usage_event,
            f"""\
<red>Could not find Checkpoint `{checkpoint_name}` (or its configuration is invalid).</red> Try running:
  - `<green>great_expectations checkpoint list</green>` to verify your Checkpoint exists
  - `<green>great_expectations checkpoint new</green>` to configure a new Checkpoint""",
        )


def select_datasource(
    context: DataContext, datasource_name: str = None
) -> Optional[BaseDatasource]:
    """Select a datasource interactively."""
    # TODO consolidate all the myriad CLI tests into this
    data_source: Optional[BaseDatasource] = None

    if datasource_name is None:
        data_sources: Dict[str, BaseDatasource] = cast(
            Dict[str, BaseDatasource],
            sorted(context.datasources.values(), key=lambda x: x.name),
        )
        if len(data_sources) == 0:
            cli_message(
                "<red>No datasources found in the context. To add a datasource, run `great_expectations datasource new`</red>"
            )
        elif len(data_sources) == 1:
            datasource_name = data_sources[0].name
        else:
            choices: str = "\n".join(
                [
                    "    {}. {}".format(i, data_source.name)
                    for i, data_source in enumerate(data_sources, 1)
                ]
            )
            option_selection: int = click.prompt(
                "Select a datasource" + "\n" + choices + "\n",
                type=click.Choice(
                    [str(i) for i, data_source in enumerate(data_sources, 1)]
                ),
                show_choices=False,
            )
            datasource_name = data_sources[int(option_selection) - 1].name

    if datasource_name is not None:
        data_source = context.get_datasource(datasource_name=datasource_name)

    return data_source


def load_data_context_with_error_handling(
    directory: str, from_cli_upgrade_command: bool = False
) -> DataContext:
    """Return a DataContext with good error handling and exit codes."""
    try:
        context: DataContext = DataContext(context_root_dir=directory)
        ge_config_version: int = context.get_config().config_version
        if (
            from_cli_upgrade_command
            and int(ge_config_version) < CURRENT_GE_CONFIG_VERSION
        ):
            directory = directory or context.root_directory
            (
                increment_version,
                exception_occurred,
            ) = upgrade_project_one_version_increment(
                context_root_dir=directory,
                ge_config_version=ge_config_version,
                continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
                from_cli_upgrade_command=from_cli_upgrade_command,
            )
            if not exception_occurred and increment_version:
                context = DataContext(context_root_dir=directory)
        return context
    except ge_exceptions.UnsupportedConfigVersionError as err:
        directory = directory or DataContext.find_context_root_dir()
        ge_config_version = DataContext.get_ge_config_version(
            context_root_dir=directory
        )
        upgrade_helper_class = (
            GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
            if ge_config_version
            else None
        )
        if upgrade_helper_class and ge_config_version < CURRENT_GE_CONFIG_VERSION:
            upgrade_project(
                context_root_dir=directory,
                ge_config_version=ge_config_version,
                from_cli_upgrade_command=from_cli_upgrade_command,
            )
        else:
            cli_message("<red>{}</red>".format(err.message))
            sys.exit(1)
    except (
        ge_exceptions.ConfigNotFoundError,
        ge_exceptions.InvalidConfigError,
    ) as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(1)
    except ge_exceptions.PluginModuleNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.PluginClassNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.InvalidConfigurationYamlError as err:
        cli_message(f"<red>{str(err)}</red>")
        sys.exit(1)


def upgrade_project(
    context_root_dir, ge_config_version, from_cli_upgrade_command=False
):
    if from_cli_upgrade_command:
        message = (
            f"<red>\nYour project appears to have an out-of-date config version ({ge_config_version}) - "
            f"the version "
            f"number must be at least {CURRENT_GE_CONFIG_VERSION}.</red>"
        )
    else:
        message = (
            f"<red>\nYour project appears to have an out-of-date config version ({ge_config_version}) - "
            f"the version "
            f"number must be at least {CURRENT_GE_CONFIG_VERSION}.\nIn order to proceed, "
            f"your project must be upgraded.</red>"
        )

    cli_message(message)
    upgrade_prompt = (
        "\nWould you like to run the Upgrade Helper to bring your project up-to-date?"
    )
    confirm_proceed_or_exit(
        confirm_prompt=upgrade_prompt,
        continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
    )
    cli_message(SECTION_SEPARATOR)

    # use loop in case multiple upgrades need to take place
    while ge_config_version < CURRENT_GE_CONFIG_VERSION:
        increment_version, exception_occurred = upgrade_project_one_version_increment(
            context_root_dir=context_root_dir,
            ge_config_version=ge_config_version,
            continuation_message=EXIT_UPGRADE_CONTINUATION_MESSAGE,
            from_cli_upgrade_command=from_cli_upgrade_command,
        )
        if exception_occurred or not increment_version:
            break
        ge_config_version += 1

    cli_message(SECTION_SEPARATOR)
    upgrade_success_message = "<green>Upgrade complete. Exiting...</green>\n"
    upgrade_incomplete_message = f"""\
<red>The Upgrade Helper was unable to perform a complete project upgrade. Next steps:</red>

    - Please perform any manual steps outlined in the Upgrade Overview and/or Upgrade Report above
    - When complete, increment the config_version key in your <cyan>great_expectations.yml</cyan> to <cyan>{
    ge_config_version + 1}</cyan>\n
To learn more about the upgrade process, visit \
<cyan>https://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html</cyan>
"""

    if ge_config_version < CURRENT_GE_CONFIG_VERSION:
        cli_message(upgrade_incomplete_message)
    else:
        cli_message(upgrade_success_message)
    sys.exit(0)


def upgrade_project_one_version_increment(
    context_root_dir: str,
    ge_config_version: float,
    continuation_message: str,
    from_cli_upgrade_command: bool = False,
) -> [bool, bool]:  # Returns increment_version, exception_occurred
    upgrade_helper_class = GE_UPGRADE_HELPER_VERSION_MAP.get(int(ge_config_version))
    if not upgrade_helper_class:
        return False, False
    target_ge_config_version = int(ge_config_version) + 1
    # set version temporarily to CURRENT_GE_CONFIG_VERSION to get functional DataContext
    DataContext.set_ge_config_version(
        config_version=CURRENT_GE_CONFIG_VERSION,
        context_root_dir=context_root_dir,
    )
    upgrade_helper = upgrade_helper_class(context_root_dir=context_root_dir)
    upgrade_overview, confirmation_required = upgrade_helper.get_upgrade_overview()

    if confirmation_required or from_cli_upgrade_command:
        upgrade_confirmed = confirm_proceed_or_exit(
            confirm_prompt=upgrade_overview,
            continuation_message=continuation_message,
            exit_on_no=False,
        )
    else:
        upgrade_confirmed = True

    if upgrade_confirmed:
        cli_message("\nUpgrading project...")
        cli_message(SECTION_SEPARATOR)
        # run upgrade and get report of what was done, if version number should be incremented
        (
            upgrade_report,
            increment_version,
            exception_occurred,
        ) = upgrade_helper.upgrade_project()
        # display report to user
        cli_message(upgrade_report)
        if exception_occurred:
            # restore version number to current number
            DataContext.set_ge_config_version(
                ge_config_version, context_root_dir, validate_config_version=False
            )
            # display report to user
            return False, True
        # set config version to target version
        if increment_version:
            DataContext.set_ge_config_version(
                target_ge_config_version,
                context_root_dir,
                validate_config_version=False,
            )
            return True, False
        # restore version number to current number
        DataContext.set_ge_config_version(
            ge_config_version, context_root_dir, validate_config_version=False
        )
        return False, False

    # restore version number to current number
    DataContext.set_ge_config_version(
        ge_config_version, context_root_dir, validate_config_version=False
    )
    cli_message(continuation_message)
    sys.exit(0)


def confirm_proceed_or_exit(
    confirm_prompt: str = "Would you like to proceed?",
    continuation_message: str = "Ok, exiting now. You can always read more at https://docs.greatexpectations.io/ !",
    exit_on_no: bool = True,
    exit_code: int = 0,
) -> Optional[bool]:
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
            cli_message(continuation_message_colorized)
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

    if config_file_location is not None and config_file_location != "":

        config_file_location_path = Path(config_file_location)

        # If the file or directory exists, treat it appropriately
        # This handles files without extensions
        if config_file_location_path.is_file():
            filename: Optional[str] = fr"{str(config_file_location_path.name)}"
            directory: Optional[str] = fr"{str(config_file_location_path.parent)}"
        elif config_file_location_path.is_dir():
            filename: Optional[str] = None
            directory: Optional[str] = config_file_location

        else:
            raise ge_exceptions.ConfigNotFoundError()

    else:
        # Return None if config_file_location is empty rather than default output of ""
        directory = None
        filename = None

    return {"directory": directory, "filename": filename}


def send_usage_message(
    data_context: DataContext,
    event: str,
    event_payload: Optional[dict] = None,
    success: bool = False,
):
    if event_payload is None:
        event_payload = {}
    event_payload.update({"api_version": "v3"})
    send_usage_stats_message(
        data_context=data_context,
        event=event,
        event_payload=event_payload,
        success=success,
    )


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
