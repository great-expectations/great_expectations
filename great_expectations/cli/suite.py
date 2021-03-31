import json
import os
import sys
from typing import Any, Dict, List, Optional, Union

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.batch_request import (
    get_batch_request,
    standardize_batch_request_display_ordering,
)
from great_expectations.cli.mark import Mark as mark
from great_expectations.cli.pretty_printing import (
    cli_message,
    cli_message_list,
    display_not_implemented_message_and_exit,
)
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.usage_statistics import (
    edit_expectation_suite_usage_statistics,
)
from great_expectations.render.renderer.v3.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)
from great_expectations.render.renderer.v3.suite_scaffold_notebook_renderer import (
    SuiteScaffoldNotebookRenderer,
)

json_parse_exception = json.decoder.JSONDecodeError

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.group()
@click.pass_context
def suite(ctx):
    """Expectation Suite operations"""
    directory: str = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    context: DataContext = toolkit.load_data_context_with_error_handling(
        directory=directory,
        from_cli_upgrade_command=False,
    )
    # TODO consider moving this all the way up in to the CLIState constructor
    ctx.obj.data_context = context


@suite.command(name="new")
@click.option(
    "--suite",
    "-es",
    default=None,
    help="Expectation suite name.",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    help="""Use to specify a batch of data to create expectations against.  Assumed with --scaffold flag.
Incompatible with --batch-request option.
""",
)
@click.option(
    "--scaffold",
    "-sf",
    is_flag=True,
    default=False,
    help="""Generate a starting expectation suite automatically so you can refine it further.
The interactive mode is assumed.  Incompatible with --batch-request option.
""",
)
@click.option(
    "--no-jupyter",
    "-nj",
    is_flag=True,
    default=False,
    help="By default launch jupyter notebooks, unless you specify the --no-jupyter flag.",
)
# TODO: <Alex>ALEX -- Can we take this option away?  It is difficult to use.</Alex>
@click.option(
    "--batch-request",
    "-br",
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a valid JSON dictionary.
Make sure to escape quotes.  Example: "{\"datasource_name\": \"my_ds\", \"data_connector_name\": \"my_connector\", \"data_asset_name\": \"my_asset\"}"
Incompatible with the interactive mode.
""",
    default=None,
)
@click.pass_context
def suite_new(ctx, suite, interactive, scaffold, no_jupyter, batch_request):
    """
    Create a new empty Expectation Suite.
    Edit in jupyter notebooks, or skip with the --no-jupyter flag.
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.new"
    _suite_new(
        context=context,
        suite_name=suite,
        interactive=interactive,
        scaffold=scaffold,
        no_jupyter=no_jupyter,
        usage_event=usage_event,
        batch_request=batch_request,
    )


def _suite_new(
    context: DataContext,
    suite_name: str,
    interactive: bool,
    scaffold: bool,
    no_jupyter: bool,
    usage_event: str,
    batch_request: Optional[Union[str, Dict[str, Union[str, Dict[str, Any]]]]] = None,
) -> None:
    if not interactive and scaffold:
        raise ValueError("The --scaffold flag assumes the interactive mode.")

    if interactive and (batch_request is not None):
        raise ValueError(
            "The --batch-request JSON option is incompatible with the interactive flag."
        )

    datasource_name: Optional[str] = None
    data_connector_name: Optional[str] = None
    data_asset_name: Optional[str] = None

    try:
        if batch_request is not None:
            batch_request = json.loads(batch_request)

        # TODO: <Alex>ALEX -- Can we be more precise about the type of profiling results in V3?</Alex>
        profiling_results: dict
        suite_name, batch_request, profiling_results = toolkit.create_expectation_suite(
            context=context,
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            expectation_suite_name=suite_name,
            interactive=interactive,
            scaffold=scaffold,
            additional_batch_request_args={"limit": 1000},
        )
        if not no_jupyter:
            cli_message(
                """<green>Opening a notebook for you now to edit your expectation suite!
If you wish to avoid this you can add the `--no-jupyter` flag.</green>\n\n"""
            )
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=True
        )

        usage_event = "cli.suite.edit"  # or else we will be sending `cli.suite.new` which is incorrect
        _suite_edit(
            context=context,
            suite_name=suite_name,
            no_jupyter=no_jupyter,
            batch_request=batch_request,
            usage_event=usage_event,
            interactive=interactive,
            datasource=datasource_name,
            suppress_usage_message=True,  # do not want to actually send usage_message, since the function call is not the result of actual usage
        )
    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        OSError,
        SQLAlchemyError,
    ) as e:
        cli_message("<red>{}</red>".format(e))
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        sys.exit(1)
    except Exception as e:
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        raise e


@suite.command(name="edit")
@click.argument("suite")
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    help="""Allows to specify explicitly whether or not a batch of data is available to reason about using the language
of expectations; otherwise, best effort is made to determine this automatically (falling back to False).
Incompatible with the --batch-request option.
""",
)
@click.option(
    "--datasource",
    "-ds",
    default=None,
    help="""The name of the datasource.  The interactive mode is assumed.  Incompatible with the --batch-request option.
""",
)
@click.option(
    "--no-jupyter",
    "-nj",
    is_flag=True,
    default=False,
    help="By default launch jupyter notebooks, unless you specify the --no-jupyter flag.",
)
# TODO: <Alex>ALEX -- Can we take this option away?  It is difficult to use.</Alex>
@click.option(
    "--batch-request",
    "-br",
    default=None,
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a valid JSON dictionary.
Make sure to escape quotes.  Example: "{\"datasource_name\": \"my_ds\", \"data_connector_name\": \"my_connector\", \"data_asset_name\": \"my_asset\"}"
Incompatible with the interactive mode.
""",
)
@click.pass_context
def suite_edit(ctx, suite, interactive, datasource, no_jupyter, batch_request):
    """
    Generate a Jupyter notebook for editing an existing Expectation Suite.

    The SUITE argument is required. This is the name you gave to the suite
    when you created it.

    The edit command will help you specify a batch interactively. Or you can
    specify them manually by providing --batch-request in valid JSON format.

    Read more about specifying batches of data in the documentation: https://docs.greatexpectations.io/
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.edit"
    _suite_edit(
        context=context,
        suite_name=suite,
        no_jupyter=no_jupyter,
        batch_request=batch_request,
        usage_event=usage_event,
        interactive=interactive,
        datasource=datasource,
        suppress_usage_message=False,
    )


def _suite_edit(
    context: DataContext,
    suite_name: str,
    no_jupyter: bool,
    usage_event: str,
    interactive: Optional[bool] = False,
    datasource: Optional[str] = None,
    suppress_usage_message: Optional[bool] = False,
    batch_request: Optional[Union[str, Dict[str, Union[str, Dict[str, Any]]]]] = None,
):
    # suppress_usage_message flag is for the situation where _suite_edit is called by _suite_new().
    # when called by _suite_new(), the flag will be set to False, otherwise it will default to True
    valid_batch_request: Optional[bool] = False
    batch_request_json: Optional[str] = None
    if isinstance(batch_request, str):
        batch_request_json = batch_request
        batch_request = None
        interactive = False

    if interactive and (batch_request_json is not None):
        raise ValueError(
            "The --batch-request JSON option is incompatible with the interactive flag."
        )

    if not interactive and datasource:
        raise ValueError("The --datasource option assumes the interactive mode.")

    try:
        if batch_request_json:
            try:
                batch_request = json.loads(batch_request_json)
                interactive = False
                if datasource:
                    batch_request["datasource_name"] = datasource
            except json_parse_exception as je:
                cli_message(
                    "<red>Please check that your batch_request contains valid JSON.\n{}</red>".format(
                        je
                    )
                )
                if not suppress_usage_message:
                    toolkit.send_usage_message(
                        data_context=context, event=usage_event, success=False
                    )
                sys.exit(1)
            except ge_exceptions.DataContextError:
                cli_message(
                    "<red>Please check that your batch_request is able to load a batch.</red>"
                )
                if not suppress_usage_message:
                    toolkit.send_usage_message(
                        data_context=context, event=usage_event, success=False
                    )
                sys.exit(1)
            except ValueError as ve:
                cli_message(
                    "<red>Please check that your batch_request is able to load a batch.\n{}</red>".format(
                        ve
                    )
                )
                if not suppress_usage_message:
                    toolkit.send_usage_message(
                        data_context=context, event=usage_event, success=False
                    )
                sys.exit(1)

        if interactive and not (
            batch_request
            and isinstance(batch_request, dict)
            and BatchRequest(**batch_request)
        ):
            cli_message(
                """
A batch of data is required to edit the suite - let's help you to specify it."""
            )

            try:
                datasource = toolkit.select_datasource(
                    context=context, datasource_name=datasource
                )
            except ValueError as ve:
                cli_message("<red>{}</red>".format(ve))
                toolkit.send_usage_message(
                    data_context=context, event=usage_event, success=False
                )
                sys.exit(1)

            if not datasource:
                cli_message("<red>No datasources found in the context.</red>")
                if not suppress_usage_message:
                    toolkit.send_usage_message(
                        data_context=context, event=usage_event, success=False
                    )
                sys.exit(1)

            (
                datasource_name,
                data_connector_name,
                data_asset_name,
                batch_request,
            ) = get_batch_request(
                context=context,
                datasource_name=datasource.name,
                data_connector_name=None,
                additional_batch_request_args=None,
            )

        if (
            batch_request
            and isinstance(batch_request, dict)
            and BatchRequest(**batch_request)
        ):
            valid_batch_request = True
            batch_request = standardize_batch_request_display_ordering(
                batch_request=batch_request
            )

        suite: ExpectationSuite = toolkit.load_expectation_suite(
            context=context, suite_name=suite_name, usage_event=usage_event
        )
        suite.add_citation(
            comment="Updated suite added via CLI",
            interactive=interactive,
            batch_request=batch_request,
        )
        context.save_expectation_suite(expectation_suite=suite)

        notebook_name: str = "edit_{}.ipynb".format(suite.expectation_suite_name)
        notebook_path: str = _get_notebook_path(context, notebook_name)
        SuiteEditNotebookRenderer.from_data_context(
            data_context=context
        ).render_to_disk(
            suite=suite,
            notebook_file_path=notebook_path,
            valid_batch_request=valid_batch_request,
            batch_request=batch_request,
        )

        if no_jupyter:
            cli_message(
                f"To continue editing this suite, run <green>jupyter notebook {notebook_path}</green>"
            )

        payload: dict = edit_expectation_suite_usage_statistics(
            data_context=context, expectation_suite_name=suite.expectation_suite_name
        )

        if not suppress_usage_message:
            toolkit.send_usage_message(
                data_context=context,
                event=usage_event,
                event_payload=payload,
                success=True,
            )

        if not no_jupyter:
            toolkit.launch_jupyter_notebook(notebook_path=notebook_path)

    except Exception as e:
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        raise e


@mark.cli_as_deprecation
@suite.command(name="demo")
@click.pass_context
def suite_demo(ctx):
    """This command is not supported in the v3 (Batch Request) API."""
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.demo"
    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)
    cli_message(
        "This command is not supported in the v3 (Batch Request) API. Please use `suite new` instead."
    )


@suite.command(name="delete")
@click.argument("suite")
@mark.cli_as_experimental
@click.pass_context
def suite_delete(ctx, suite):
    """
    Delete an expectation suite from the expectation store.
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.delete"
    try:
        suite_names: List[str] = context.list_expectation_suite_names()
    except Exception as e:
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        raise e
    if not suite_names:
        toolkit.exit_with_failure_message_and_stats(
            context=context,
            usage_event=usage_event,
            message="</red>No expectation suites found in the project.</red>",
        )

    if suite not in suite_names:
        toolkit.exit_with_failure_message_and_stats(
            context=context,
            usage_event=usage_event,
            message=f"No expectation suite named {suite} found.",
        )

    if not (ctx.obj.assume_yes or toolkit.confirm_proceed_or_exit(exit_on_no=False)):
        cli_message(f"Suite `{suite}` was not deleted.")
        sys.exit(0)

    context.delete_expectation_suite(suite)
    cli_message(f"Deleted the expectation suite named: {suite}")
    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)


@suite.command(name="scaffold")
@click.argument("suite")
@click.option(
    "--jupyter/--no-jupyter",
    is_flag=True,
    help="By default launch jupyter notebooks unless you specify the --no-jupyter flag",
    default=True,
)
@mark.cli_as_experimental
@click.pass_context
def suite_scaffold(ctx, suite, no_jupyter):
    """Scaffold a new Expectation Suite."""
    display_not_implemented_message_and_exit()
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    _suite_scaffold(suite, directory, no_jupyter)


def _suite_scaffold(suite: str, directory: str, no_jupyter: bool) -> None:
    usage_event = "cli.suite.scaffold"
    suite_name = suite
    context = toolkit.load_data_context_with_error_handling(directory)
    notebook_filename = f"scaffold_{suite_name}.ipynb"
    notebook_path = _get_notebook_path(context, notebook_filename)

    if suite_name in context.list_expectation_suite_names():
        toolkit.tell_user_suite_exists(suite_name)
        if os.path.isfile(notebook_path):
            cli_message(
                f"  - If you wish to adjust your scaffolding, you can open this notebook with jupyter: `{notebook_path}` <red>(Please note that if you run that notebook, you will overwrite your existing suite.)</red>"
            )
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        sys.exit(1)

    datasource = toolkit.select_datasource(context)
    if datasource is None:
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        sys.exit(1)

    _suite = context.create_expectation_suite(suite_name)
    _, _, _, batch_kwargs = get_batch_request(
        context=context, datasource_name=datasource.name
    )
    renderer = SuiteScaffoldNotebookRenderer(context, _suite, batch_kwargs)
    renderer.render_to_disk(notebook_path)

    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)

    if not no_jupyter:
        toolkit.launch_jupyter_notebook(notebook_path)
    else:
        cli_message(
            f"To continue scaffolding this suite, run `jupyter notebook {notebook_path}`"
        )


@suite.command(name="list")
@click.pass_context
def suite_list(ctx):
    """Lists available Expectation Suites."""
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.list"
    try:
        suite_names: List[str] = context.list_expectation_suite_names()
    except Exception as e:
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        raise e

    suite_names_styled: List[str] = [
        f" - <cyan>{suite_name}</cyan>" for suite_name in suite_names
    ]
    if len(suite_names_styled) == 0:
        cli_message("No Expectation Suites found")
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=True
        )
        return

    list_intro_string: str
    if len(suite_names_styled) == 1:
        list_intro_string = "1 Expectation Suite found:"
    else:
        list_intro_string = f"{len(suite_names_styled)} Expectation Suites found:"
    cli_message_list(suite_names_styled, list_intro_string)
    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)


def _get_notebook_path(context, notebook_name):
    return os.path.abspath(
        os.path.join(
            context.root_directory, context.GE_EDIT_NOTEBOOK_DIR, notebook_name
        )
    )
