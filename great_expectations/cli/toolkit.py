import datetime
import os
import subprocess
import sys
from typing import Union

import click
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.datasource import get_batch_kwargs
from great_expectations.cli.docs import build_docs
from great_expectations.cli.util import cli_message
from great_expectations.core import ExpectationSuite
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.datasource import Datasource
from great_expectations.exceptions import CheckpointError, CheckpointNotFoundError
from great_expectations.profile import BasicSuiteBuilderProfiler


def create_expectation_suite(
    context,
    datasource_name=None,
    batch_kwargs_generator_name=None,
    generator_asset=None,
    batch_kwargs=None,
    expectation_suite_name=None,
    additional_batch_kwargs=None,
    empty_suite=False,
    show_intro_message=False,
    open_docs=False,
    profiler_configuration="demo",
):
    """
    Create a new expectation suite.

    :return: a tuple: (success, suite name)
    """
    if show_intro_message and not empty_suite:
        cli_message(
            "\n<cyan>========== Create sample Expectations ==========</cyan>\n\n"
        )

    data_source = select_datasource(context, datasource_name=datasource_name)
    if data_source is None:
        # select_datasource takes care of displaying an error message, so all is left here is to exit.
        sys.exit(1)

    datasource_name = data_source.name

    if expectation_suite_name in context.list_expectation_suite_names():
        tell_user_suite_exists(expectation_suite_name)
        sys.exit(1)

    if (
        batch_kwargs_generator_name is None
        or generator_asset is None
        or batch_kwargs is None
    ):
        (
            datasource_name,
            batch_kwargs_generator_name,
            generator_asset,
            batch_kwargs,
        ) = get_batch_kwargs(
            context,
            datasource_name=datasource_name,
            batch_kwargs_generator_name=batch_kwargs_generator_name,
            generator_asset=generator_asset,
            additional_batch_kwargs=additional_batch_kwargs,
        )
        # In this case, we have "consumed" the additional_batch_kwargs
        additional_batch_kwargs = {}

    if expectation_suite_name is None:
        default_expectation_suite_name = _get_default_expectation_suite_name(
            batch_kwargs, generator_asset
        )
        while True:
            expectation_suite_name = click.prompt(
                "\nName the new expectation suite",
                default=default_expectation_suite_name,
            )
            if expectation_suite_name in context.list_expectation_suite_names():
                tell_user_suite_exists(expectation_suite_name)
            else:
                break

    if empty_suite:
        create_empty_suite(context, expectation_suite_name, batch_kwargs)
        return True, expectation_suite_name

    profiling_results = _profile_to_create_a_suite(
        additional_batch_kwargs,
        batch_kwargs,
        batch_kwargs_generator_name,
        context,
        datasource_name,
        expectation_suite_name,
        generator_asset,
        profiler_configuration,
    )

    build_docs(context, view=False)
    if open_docs:
        _attempt_to_open_validation_results_in_data_docs(context, profiling_results)

    return True, expectation_suite_name


def _profile_to_create_a_suite(
    additional_batch_kwargs,
    batch_kwargs,
    batch_kwargs_generator_name,
    context,
    datasource_name,
    expectation_suite_name,
    generator_asset,
    profiler_configuration,
):
    click.prompt(
        """
Great Expectations will choose a couple of columns and generate expectations about them
to demonstrate some examples of assertions you can make about your data.

Press Enter to continue
""",
        default=True,
        show_default=False,
    )
    # TODO this may not apply
    cli_message("\nGenerating example Expectation Suite...")
    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    profiling_results = context.profile_data_asset(
        datasource_name,
        batch_kwargs_generator_name=batch_kwargs_generator_name,
        data_asset_name=generator_asset,
        batch_kwargs=batch_kwargs,
        profiler=BasicSuiteBuilderProfiler,
        profiler_configuration=profiler_configuration,
        expectation_suite_name=expectation_suite_name,
        run_id=run_id,
        additional_batch_kwargs=additional_batch_kwargs,
    )
    if not profiling_results["success"]:
        _raise_profiling_errors(profiling_results)
    return profiling_results


def _raise_profiling_errors(profiling_results):
    if (
        profiling_results["error"]["code"]
        == DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND
    ):
        raise ge_exceptions.DataContextError(
            """Some of the data assets you specified were not found: {0:s}
            """.format(
                ",".join(profiling_results["error"]["not_found_data_assets"])
            )
        )
    raise ge_exceptions.DataContextError(
        "Unknown profiling error code: " + profiling_results["error"]["code"]
    )


def _attempt_to_open_validation_results_in_data_docs(context, profiling_results):
    try:
        # TODO this is really brittle and not covered in tests
        validation_result = profiling_results["results"][0][1]
        validation_result_identifier = ValidationResultIdentifier.from_object(
            validation_result
        )
        context.open_data_docs(resource_identifier=validation_result_identifier)
    except (KeyError, IndexError):
        context.open_data_docs()


def _get_default_expectation_suite_name(batch_kwargs, generator_asset):
    if generator_asset:
        suite_name = f"{generator_asset}.warning"
    elif "query" in batch_kwargs:
        suite_name = "query.warning"
    elif "path" in batch_kwargs:
        try:
            # Try guessing a filename
            filename = os.path.split(os.path.normpath(batch_kwargs["path"]))[1]
            # Take all but the last part after the period
            filename = ".".join(filename.split(".")[:-1])
            suite_name = str(filename) + ".warning"
        except (OSError, IndexError):
            suite_name = "warning"
    else:
        suite_name = "warning"
    return suite_name


def tell_user_suite_exists(suite_name: str) -> None:
    cli_message(
        f"""<red>An expectation suite named `{suite_name}` already exists.</red>
  - If you intend to edit the suite please use `great_expectations suite edit {suite_name}`."""
    )


def create_empty_suite(
    context: DataContext, expectation_suite_name: str, batch_kwargs
) -> None:
    suite = context.create_expectation_suite(expectation_suite_name)
    suite.add_citation(comment="New suite added via CLI", batch_kwargs=batch_kwargs)
    context.save_expectation_suite(suite, expectation_suite_name)


def launch_jupyter_notebook(notebook_path: str) -> None:
    jupyter_command_override = os.getenv("GE_JUPYTER_CMD", None)
    if jupyter_command_override:
        subprocess.call(f"{jupyter_command_override} {notebook_path}", shell=True)
    else:
        subprocess.call(["jupyter", "notebook", notebook_path])


def load_batch(
    context: DataContext,
    suite: Union[str, ExpectationSuite],
    batch_kwargs: Union[dict, BatchKwargs],
) -> DataAsset:
    batch: DataAsset = context.get_batch(batch_kwargs, suite)
    assert isinstance(
        batch, DataAsset
    ), "Batch failed to load. Please check your batch_kwargs"
    return batch


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
        suite = context.get_expectation_suite(suite_name)
        return suite
    except ge_exceptions.DataContextError as e:
        exit_with_failure_message_and_stats(
            context,
            usage_event,
            f"<red>Could not find a suite named `{suite_name}`.</red> Please check "
            "the name by running `great_expectations suite list` and try again.",
        )


def exit_with_failure_message_and_stats(
    context: DataContext, usage_event: str, message: str
) -> None:
    cli_message(message)
    send_usage_message(context, event=usage_event, success=False)
    sys.exit(1)


def load_checkpoint(
    context: DataContext, checkpoint_name: str, usage_event: str
) -> dict:
    """Load a checkpoint or raise helpful errors."""
    try:
        checkpoint_config = context.get_checkpoint(checkpoint_name)
        return checkpoint_config
    except CheckpointNotFoundError as e:
        exit_with_failure_message_and_stats(
            context,
            usage_event,
            f"""\
<red>Could not find checkpoint `{checkpoint_name}`.</red> Try running:
  - `<green>great_expectations checkpoint list</green>` to verify your checkpoint exists
  - `<green>great_expectations checkpoint new</green>` to configure a new checkpoint""",
        )
    except CheckpointError as e:
        exit_with_failure_message_and_stats(context, usage_event, f"<red>{e}</red>")


def select_datasource(context: DataContext, datasource_name: str = None) -> Datasource:
    """Select a datasource interactively."""
    # TODO consolidate all the myriad CLI tests into this
    data_source = None

    if datasource_name is None:
        data_sources = sorted(context.list_datasources(), key=lambda x: x["name"])
        if len(data_sources) == 0:
            cli_message(
                "<red>No datasources found in the context. To add a datasource, run `great_expectations datasource new`</red>"
            )
        elif len(data_sources) == 1:
            datasource_name = data_sources[0]["name"]
        else:
            choices = "\n".join(
                [
                    "    {}. {}".format(i, data_source["name"])
                    for i, data_source in enumerate(data_sources, 1)
                ]
            )
            option_selection = click.prompt(
                "Select a datasource" + "\n" + choices + "\n",
                type=click.Choice(
                    [str(i) for i, data_source in enumerate(data_sources, 1)]
                ),
                show_choices=False,
            )
            datasource_name = data_sources[int(option_selection) - 1]["name"]

    if datasource_name is not None:
        data_source = context.get_datasource(datasource_name)

    return data_source


def load_data_context_with_error_handling(directory: str) -> DataContext:
    """Return a DataContext with good error handling and exit codes."""
    # TODO consolidate all the myriad CLI tests into this
    try:
        context = DataContext(directory)
        return context
    except (ge_exceptions.ConfigNotFoundError, ge_exceptions.InvalidConfigError) as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(1)
    except ge_exceptions.PluginModuleNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
    except ge_exceptions.PluginClassNotFoundError as err:
        cli_message(err.cli_colored_message)
        sys.exit(1)
