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
@click.option("--suite", "-es", default=None, help="Expectation suite name.")
@click.option(
    "--no-dataset",
    "-nd",
    is_flag=True,
    default=False,
    help="""By default must be able to specify a batch of data to reason about using the language of expectations,
unless you specify the --no-dataset flag.  Incompatible with --scaffold flag and --batch-request option.
""",
)
@click.option(
    "--scaffold",
    "-sf",
    is_flag=True,
    default=False,
    help="""Generate a starting expectation suite automatically so you can refine it further.
Incompatible with the --no-dataset flag.
""",
)
@click.option(
    "--jupyter/--no-jupyter",
    "-yj/-nj",
    is_flag=True,
    default=True,
    help="By default launch jupyter notebooks, unless you specify the --no-jupyter flag.",
)
# TODO: <Alex>ALEX -- Can we take this option away?  It is difficult to use.</Alex>
@click.option(
    "--batch-request",
    "-br",
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a valid JSON dictionary.
Make sure to escape quotes.  Example: "{\"datasource_name\": \"my_ds\", \"data_connector_name\": \"my_connector\", \"data_asset_name\": \"my_asset\"}"
Incompatible with the --no-dataset flag.
""",
    default=None,
)
@click.pass_context
def suite_new(ctx, suite, no_dataset, scaffold, jupyter, batch_request):
    """
    Create a new empty Expectation Suite.
    Edit in jupyter notebooks, or skip with the --no-jupyter flag.
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.new"
    _suite_new(
        context=context,
        suite_name=suite,
        no_dataset=no_dataset,
        scaffold=scaffold,
        jupyter=jupyter,
        usage_event=usage_event,
        batch_request=batch_request,
    )


def _suite_new(
    context: DataContext,
    suite_name: str,
    no_dataset: bool,
    scaffold: bool,
    jupyter: bool,
    usage_event: str,
    batch_request: Optional[Union[str, Dict[str, Union[str, Dict[str, Any]]]]] = None,
) -> None:
    if no_dataset and (scaffold or (batch_request is not None)):
        raise ValueError(
            "The --scaffold flag and --batch-request JSON are incompatible with the --no-dataset flag."
        )

    datasource_name: Optional[str] = None
    data_connector_name: Optional[str] = None
    data_asset_name: Optional[str] = None

    try:
        if batch_request is not None:
            batch_request = json.loads(batch_request)

        # TODO: <Alex>ALEX -- Can we be more precise about the type of profiling results in V3?</Alex>
        profiling_results: dict
        suite_name, profiling_results = toolkit.create_expectation_suite(
            context=context,
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            expectation_suite_name=suite_name,
            no_dataset=no_dataset,
            scaffold=scaffold,
            additional_batch_request_args={"limit": 1000},
        )
        if jupyter:
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
            jupyter=jupyter,
            batch_request=batch_request,
            usage_event=usage_event,
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
    "--no-dataset",
    "-nd",
    is_flag=True,
    default=None,
    help="""Allows to specify explicitly whether or not a batch of data is available to reason about using the language
of expectations; otherwise, best effort is made to determine this automatically (falling back to False).
Incompatible with the --datasource and --batch-request options.
""",
)
@click.option(
    "--datasource",
    "-ds",
    default=None,
    help="""The name of the datasource.  Incompatible with the --no-dataset flag.""",
)
@click.option(
    "--jupyter/--no-jupyter",
    "-yj/-nj",
    is_flag=True,
    default=True,
    help="By default launch jupyter notebooks, unless you specify the --no-jupyter flag.",
)
# TODO: <Alex>ALEX -- Can we take this option away?  It is difficult to use.</Alex>
@click.option(
    "--batch-request",
    "-br",
    default=None,
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a valid JSON dictionary.
Make sure to escape quotes.  Example: "{\"datasource_name\": \"my_ds\", \"data_connector_name\": \"my_connector\", \"data_asset_name\": \"my_asset\"}"
Incompatible with the --no-dataset flag.
""",
)
@click.pass_context
def suite_edit(ctx, suite, no_dataset, datasource, jupyter, batch_request):
    """
    Generate a Jupyter notebook for editing an existing Expectation Suite.

    The SUITE argument is required. This is the name you gave to the suite
    when you created it.

    A batch of data is required to edit the suite, which is used as a sample.

    The edit command will help you specify a batch interactively. Or you can
    specify them manually by providing --batch-kwargs in valid JSON format.

    Read more about specifying batches of data in the documentation: https://docs.greatexpectations.io/
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.edit"
    _suite_edit(
        context=context,
        suite_name=suite,
        jupyter=jupyter,
        batch_request=batch_request,
        usage_event=usage_event,
        no_dataset=no_dataset,
        datasource=datasource,
        suppress_usage_message=False,
    )


def _suite_edit(
    context: DataContext,
    suite_name: str,
    jupyter: bool,
    usage_event: str,
    no_dataset: Optional[bool] = None,
    datasource: Optional[str] = None,
    suppress_usage_message: Optional[bool] = False,
    batch_request: Optional[Union[str, Dict[str, Union[str, Dict[str, Any]]]]] = None,
):
    if bool(no_dataset) and ((datasource is not None) or (batch_request is not None)):
        raise ValueError(
            "The --datasource name and --batch-request JSON are incompatible with the --no-dataset flag."
        )

    # suppress_usage_message flag is for the situation where _suite_edit is called by _suite_new().
    # when called by _suite_new(), the flag will be set to False, otherwise it will default to True
    batch_request_json: Optional[str] = batch_request
    batch_request = None

    try:
        suite: ExpectationSuite = toolkit.load_expectation_suite(
            context=context, suite_name=suite_name, usage_event=usage_event
        )
        citations: List[Dict[str, Any]]
        citation: Optional[Dict[str, Any]]

        citations = suite.get_citations()
        if citations:
            citation = citations[-1]
            if no_dataset is None:
                no_dataset = citation.get("no_dataset")
            if no_dataset:
                batch_request = None
            else:
                batch_request = citation.get("batch_request")
                if batch_request:
                    no_dataset = False

        if batch_request_json:
            try:
                batch_request = json.loads(batch_request_json)
                no_dataset = False
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

        no_dataset = no_dataset and (datasource is None)
        if not (no_dataset or batch_request):
            cli_message(
                """
A batch of data is required to edit the suite - let's help you to specify it."""
            )

            additional_batch_request_args: Optional[Dict[str, Any]] = None
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
                additional_batch_request_args=additional_batch_request_args,
            )

        if batch_request:
            batch_request = standardize_batch_request_display_ordering(
                batch_request=batch_request
            )

        suite.add_citation(
            comment="Updated suite added via CLI",
            no_dataset=no_dataset,
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
            no_dataset=no_dataset,
            batch_request=batch_request,
        )

        if not jupyter:
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

        if jupyter:
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
def suite_scaffold(ctx, suite, jupyter):
    """Scaffold a new Expectation Suite."""
    display_not_implemented_message_and_exit()
    directory = toolkit.parse_cli_config_file_location(
        config_file_location=ctx.obj.config_file_location
    ).get("directory")
    _suite_scaffold(suite, directory, jupyter)


def _suite_scaffold(suite: str, directory: str, jupyter: bool) -> None:
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

    if jupyter:
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
