import copy
import os
import sys
from typing import Any, Dict, List, Optional, Union

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.batch_request import get_batch_request
from great_expectations.cli.mark import Mark as mark
from great_expectations.cli.pretty_printing import cli_message, cli_message_list
from great_expectations.cli.toolkit import load_json_file_into_dict
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.usage_statistics import (
    edit_expectation_suite_usage_statistics,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.render.renderer.v3.suite_edit_notebook_renderer import (
    SuiteEditNotebookRenderer,
)
from great_expectations.render.renderer.v3.suite_profile_notebook_renderer import (
    SuiteProfileNotebookRenderer,
)

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
    "--expectation-suite",
    "-e",
    default=None,
    help="Expectation suite name.",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    help="""Indicates that a batch of data is used to create expectations against.  Required with --profile flag and
with --batch-request option.
""",
)
@click.option(
    "--profile",
    "-p",
    is_flag=True,
    default=False,
    help="""Generate a starting expectation suite automatically so you can refine it further. Requires --interactive
flag.
""",
)
@click.option(
    "--batch-request",
    "-br",
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a path to a valid JSON file.
Requires --interactive flag.
""",
    default=None,
)
@click.option(
    "--no-jupyter",
    "-nj",
    is_flag=True,
    default=False,
    help="By default launch jupyter notebooks, unless you specify --no-jupyter flag.",
)
@click.pass_context
def suite_new(ctx, expectation_suite, interactive, profile, batch_request, no_jupyter):
    """
    Create a new empty Expectation Suite.
    Edit in jupyter notebooks, or skip with the --no-jupyter flag.
    """
    context: DataContext = ctx.obj.data_context
    usage_event: str = "cli.suite.new"

    error_message: Optional[str] = None

    if not interactive and (profile or (batch_request is not None)):
        error_message = """Using --profile flag and/or --batch-request <path to JSON file> option requires \
--interactive flag.
"""

    if error_message is not None:
        cli_message(string=f"<red>{error_message}</red>")
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        sys.exit(1)

    _suite_new_workflow(
        context=context,
        expectation_suite_name=expectation_suite,
        interactive=interactive,
        profile=profile,
        no_jupyter=no_jupyter,
        usage_event=usage_event,
        batch_request=batch_request,
    )


def _suite_new_workflow(
    context: DataContext,
    expectation_suite_name: str,
    interactive: bool,
    profile: bool,
    no_jupyter: bool,
    usage_event: str,
    batch_request: Optional[str] = None,
):
    try:
        if batch_request is not None and isinstance(batch_request, str):
            batch_request = load_json_file_into_dict(
                filepath=batch_request,
                usage_event=usage_event,
                data_context=context,
            )
            try:
                batch_request = BatchRequest(**batch_request).get_json_dict()
            except TypeError as e:
                cli_message(
                    string="<red>Please check that your batch_request is valid and is able to load a batch.</red>"
                )
                cli_message(string="<red>{}</red>".format(e))
                toolkit.send_usage_message(
                    data_context=context, event=usage_event, success=False
                )
                sys.exit(1)

        datasource_name: Optional[str] = None
        data_asset_name: Optional[str] = None
        additional_batch_request_args: Optional[
            Dict[str, Union[str, int, Dict[str, Any]]]
        ] = {"limit": 1000}

        if interactive:
            if not batch_request:
                cli_message(
                    string="""A batch of data is required to edit the suite - let's help you to specify it."""
                )
                datasource: BaseDatasource = toolkit.select_datasource(
                    context=context, datasource_name=datasource_name
                )

                if datasource is None:
                    # select_datasource takes care of displaying an error message, so all is left here is to exit.
                    sys.exit(1)

                batch_request = get_batch_request(
                    datasource=datasource,
                    additional_batch_request_args=additional_batch_request_args,
                )
                # In this case, we have "consumed" the additional_batch_request_args
                additional_batch_request_args = {}

            data_asset_name = batch_request.get("data_asset_name")
        else:
            batch_request = None

        if expectation_suite_name is None:
            default_expectation_suite_name: str = (
                toolkit.get_default_expectation_suite_name(
                    data_asset_name=data_asset_name,
                    batch_request=batch_request,
                )
            )
            while True:
                expectation_suite_name = click.prompt(
                    "\nName the new Expectation Suite",
                    default=default_expectation_suite_name,
                )
                if expectation_suite_name not in context.list_expectation_suite_names():
                    break
                toolkit.tell_user_suite_exists(suite_name=expectation_suite_name)
        elif expectation_suite_name in context.list_expectation_suite_names():
            toolkit.tell_user_suite_exists(suite_name=expectation_suite_name)
            sys.exit(1)

        suite: ExpectationSuite = toolkit.load_expectation_suite(
            context=context,
            expectation_suite_name=expectation_suite_name,
            usage_event=usage_event,
            create_if_not_exist=True,
        )

        if (
            batch_request
            and isinstance(batch_request, dict)
            and BatchRequest(**batch_request)
        ):
            suite.add_citation(
                comment="Created suite added via CLI",
                batch_request=batch_request,
            )
            context.save_expectation_suite(expectation_suite=suite)

        if not no_jupyter:
            cli_message(
                string="""<green>Opening a notebook for you now to edit your expectation suite!
If you wish to avoid this you can add the `--no-jupyter` flag.</green>\n\n"""
            )

        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=True
        )

        datasource_name: Optional[str] = None
        if batch_request:
            datasource_name = batch_request.get("datasource_name")

        usage_event = "cli.suite.edit"  # or else we will be sending `cli.suite.new` which is incorrect
        _suite_edit_workflow(
            context=context,
            expectation_suite_name=expectation_suite_name,
            profile=profile,
            usage_event=usage_event,
            interactive=interactive,
            no_jupyter=no_jupyter,
            create_if_not_exist=True,
            datasource_name=datasource_name,
            batch_request=batch_request,
            additional_batch_request_args=additional_batch_request_args,
            suppress_usage_message=True,  # do not want to actually send usage_message, since the function call is not the result of actual usage
        )
    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        ValueError,
        OSError,
        SQLAlchemyError,
    ) as e:
        cli_message(string="<red>{}</red>".format(e))
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
@click.argument("expectation_suite")
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    help="""Allows to specify explicitly whether or not a batch of data is available to reason about using the language
of expectations; otherwise, best effort is made to determine this automatically (falling back to False).  Required with
--datasource-name option and with --batch-request option.
""",
)
@click.option(
    "--datasource-name",
    "-ds",
    default=None,
    help="""The name of the datasource. Requires --interactive flag.  Incompatible with --batch-request option.
""",
)
@click.option(
    "--batch-request",
    "-br",
    help="""Arguments to be provided to get_batch when loading the data asset.  Must be a path to a valid JSON file.
Requires --interactive flag.  Incompatible with --datasource-name option.
""",
    default=None,
)
@click.option(
    "--no-jupyter",
    "-nj",
    is_flag=True,
    default=False,
    help="By default launch jupyter notebooks, unless you specify --no-jupyter flag.",
)
@click.pass_context
def suite_edit(
    ctx, expectation_suite, interactive, datasource_name, batch_request, no_jupyter
):
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

    error_message: Optional[str] = None

    if not interactive and (
        (datasource_name is not None) or (batch_request is not None)
    ):
        error_message = """Using --datasource-name DATASOURCE_NAME option or --batch-request <path to JSON file> \
option requires --interactive flag.
"""

    if interactive and (datasource_name is not None) and (batch_request is not None):
        error_message = """Only one of --datasource-name DATASOURCE_NAME and --batch-request <path to JSON file> \
options can be used.
"""

    if error_message is not None:
        cli_message(string=f"<red>{error_message}</red>")
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=False
        )
        sys.exit(1)

    additional_batch_request_args: Optional[
        Dict[str, Union[str, int, Dict[str, Any]]]
    ] = {"limit": 1000}

    _suite_edit_workflow(
        context=context,
        expectation_suite_name=expectation_suite,
        profile=False,
        usage_event=usage_event,
        interactive=interactive,
        no_jupyter=no_jupyter,
        create_if_not_exist=False,
        datasource_name=datasource_name,
        batch_request=batch_request,
        additional_batch_request_args=additional_batch_request_args,
        suppress_usage_message=False,
    )


def _suite_edit_workflow(
    context: DataContext,
    expectation_suite_name: str,
    profile: bool,
    usage_event: str,
    interactive: bool,
    no_jupyter: bool,
    create_if_not_exist: Optional[bool] = False,
    datasource_name: Optional[str] = None,
    batch_request: Optional[
        Union[str, Dict[str, Union[str, int, Dict[str, Any]]]]
    ] = None,
    additional_batch_request_args: Optional[
        Dict[str, Union[str, int, Dict[str, Any]]]
    ] = None,
    suppress_usage_message: Optional[bool] = False,
):
    # suppress_usage_message flag is for the situation where _suite_edit_workflow is called by _suite_new_workflow().
    # when called by _suite_new_workflow(), the flag will be set to True, otherwise it will default to False
    if suppress_usage_message:
        usage_event = None

    suite: ExpectationSuite = toolkit.load_expectation_suite(
        context=context,
        expectation_suite_name=expectation_suite_name,
        usage_event=usage_event,
        create_if_not_exist=create_if_not_exist,
    )

    try:
        if interactive:
            batch_request_from_citation_is_up_to_date: bool = True

            batch_request_from_citation: Optional[
                Union[str, Dict[str, Union[str, Dict[str, Any]]]]
            ] = toolkit.get_batch_request_from_citations(expectation_suite=suite)

            if batch_request is not None and isinstance(batch_request, str):
                batch_request = toolkit.get_batch_request_from_json_file(
                    batch_request_json_file_path=batch_request,
                    data_context=context,
                    usage_event=usage_event,
                    suppress_usage_message=suppress_usage_message,
                )
                if batch_request != batch_request_from_citation:
                    batch_request_from_citation_is_up_to_date = False

            if not (
                batch_request
                and isinstance(batch_request, dict)
                and BatchRequest(**batch_request)
            ):
                if (
                    batch_request_from_citation
                    and isinstance(batch_request_from_citation, dict)
                    and BatchRequest(**batch_request_from_citation)
                ):
                    batch_request = copy.deepcopy(batch_request_from_citation)
                else:
                    cli_message(
                        string="""
A batch of data is required to edit the suite - let's help you to specify it."""
                    )

                    datasource = toolkit.select_datasource(
                        context=context, datasource_name=datasource_name
                    )

                    if not datasource:
                        cli_message(
                            string="<red>No datasources found in the context.</red>"
                        )
                        if not suppress_usage_message:
                            toolkit.send_usage_message(
                                data_context=context, event=usage_event, success=False
                            )
                        sys.exit(1)

                    batch_request = get_batch_request(
                        datasource=datasource,
                        additional_batch_request_args=additional_batch_request_args,
                    )

                    if batch_request != batch_request_from_citation:
                        batch_request_from_citation_is_up_to_date = False

            if (
                not batch_request_from_citation_is_up_to_date
                and batch_request
                and isinstance(batch_request, dict)
                and BatchRequest(**batch_request)
            ):
                suite.add_citation(
                    comment="Updated suite added via CLI",
                    batch_request=batch_request,
                )
                context.save_expectation_suite(expectation_suite=suite)

        notebook_name: str = "edit_{}.ipynb".format(expectation_suite_name)
        notebook_path: str = _get_notebook_path(context, notebook_name)

        if profile:
            if not (
                batch_request
                and isinstance(batch_request, dict)
                and BatchRequest(**batch_request)
            ):
                cli_message(
                    string="<red>Creating expectations with --profile flag requires a valid batch of data.</red>"
                )
                if not suppress_usage_message:
                    toolkit.send_usage_message(
                        data_context=context, event=usage_event, success=False
                    )
                sys.exit(1)
            renderer: SuiteProfileNotebookRenderer = SuiteProfileNotebookRenderer(
                context=context,
                expectation_suite_name=expectation_suite_name,
                batch_request=batch_request,
            )
            renderer.render_to_disk(notebook_file_path=notebook_path)
        else:
            SuiteEditNotebookRenderer.from_data_context(
                data_context=context
            ).render_to_disk(
                suite=suite,
                notebook_file_path=notebook_path,
                batch_request=batch_request,
            )

        if no_jupyter:
            cli_message(
                string=f"To continue editing this suite, run <green>jupyter notebook {notebook_path}</green>"
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

    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        ValueError,
        OSError,
        SQLAlchemyError,
    ) as e:
        cli_message(string="<red>{}</red>".format(e))
        if not suppress_usage_message:
            toolkit.send_usage_message(
                data_context=context, event=usage_event, success=False
            )
        sys.exit(1)

    except Exception as e:
        if not suppress_usage_message:
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
        string="This command is not supported in the v3 (Batch Request) API. Please use `suite new` instead."
    )


@suite.command(name="delete")
@click.argument("suite")
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
        cli_message(string=f"Suite `{suite}` was not deleted.")
        sys.exit(0)

    context.delete_expectation_suite(suite)
    cli_message(string=f"Deleted the expectation suite named: {suite}")
    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)


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
        cli_message(string="No Expectation Suites found")
        toolkit.send_usage_message(
            data_context=context, event=usage_event, success=True
        )
        return

    list_intro_string: str
    if len(suite_names_styled) == 1:
        list_intro_string = "1 Expectation Suite found:"
    else:
        list_intro_string = f"{len(suite_names_styled)} Expectation Suites found:"
    cli_message_list(
        string_list=suite_names_styled, list_intro_string=list_intro_string
    )
    toolkit.send_usage_message(data_context=context, event=usage_event, success=True)


def _get_notebook_path(context, notebook_name):
    return os.path.abspath(
        os.path.join(
            context.root_directory, context.GE_EDIT_NOTEBOOK_DIR, notebook_name
        )
    )
