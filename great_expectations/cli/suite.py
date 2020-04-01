import json
import os
import subprocess
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.datasource import (
    create_expectation_suite as create_expectation_suite_impl,
)
from great_expectations.cli.datasource import (
    get_batch_kwargs,
    select_datasource,
)
from great_expectations.cli.util import cli_message, load_expectation_suite, cli_message_list
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message, _anonymizers, \
    edit_expectation_suite_usage_statistics
from great_expectations.data_asset import DataAsset
from great_expectations.render.renderer.notebook_renderer import NotebookRenderer

json_parse_exception = json.decoder.JSONDecodeError

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.group()
def suite():
    """Expectation Suite operations"""
    pass


@suite.command(name="edit")
@click.argument("suite")
@click.option(
    "--datasource",
    "-ds",
    default=None,
    help="""The name of the datasource. The datasource must contain a single BatchKwargGenerator that can list data assets in the datasource """,
)
@click.option(
    "--batch-kwargs",
    default=None,
    help="""Batch_kwargs that specify the batch of data to be used a sample when editing the suite. Must be a valid JSON dictionary.
Make sure to escape quotes. Example: "{\"datasource\": \"my_db\", \"query\": \"select * from my_table\"}"    
""",
)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--jupyter/--no-jupyter",
    is_flag=True,
    help="By default launch jupyter notebooks unless you specify the --no-jupyter flag",
    default=True,
)
def suite_edit(suite, datasource, directory, jupyter, batch_kwargs):
    """
    Generate a Jupyter notebook for editing an existing Expectation Suite.

    The SUITE argument is required. This is the name you gave to the suite
    when you created it.

    A batch of data is required to edit the suite, which is used as a sample.

    The edit command will help you specify a batch interactively. Or you can
    specify them manually by providing --batch-kwargs in valid JSON format.

    Read more about specifying batches of data in the documentation: https://docs.greatexpectations.io/
    """
    _suite_edit(suite, datasource, directory, jupyter, batch_kwargs)


def _suite_edit(suite, datasource, directory, jupyter, batch_kwargs):
    batch_kwargs_json = batch_kwargs
    batch_kwargs = None
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    try:
        suite = load_expectation_suite(context, suite)
        citations = suite.get_citations(sort=True, require_batch_kwargs=True)

        if batch_kwargs_json:
            try:
                batch_kwargs = json.loads(batch_kwargs_json)
                if datasource:
                    batch_kwargs["datasource"] = datasource
                _batch = context.get_batch(batch_kwargs, suite.expectation_suite_name)
                assert isinstance(_batch, DataAsset)
            except json_parse_exception as je:
                cli_message(
                    "<red>Please check that your batch_kwargs are valid JSON.\n{}</red>".format(
                        je
                    )
                )
                send_usage_message(
                    data_context=context,
                    event="cli.suite.edit",
                    success=False
                )
                sys.exit(1)
            except ge_exceptions.DataContextError:
                cli_message(
                    "<red>Please check that your batch_kwargs are able to load a batch.</red>"
                )
                send_usage_message(
                    data_context=context,
                    event="cli.suite.edit",
                    success=False
                )
                sys.exit(1)
            except ValueError as ve:
                cli_message(
                    "<red>Please check that your batch_kwargs are able to load a batch.\n{}</red>".format(
                        ve
                    )
                )
                send_usage_message(
                    data_context=context,
                    event="cli.suite.edit",
                    success=False
                )
                sys.exit(1)
        elif citations:
            citation = citations[-1]
            batch_kwargs = citation.get("batch_kwargs")

        if not batch_kwargs:
            cli_message(
            """
A batch of data is required to edit the suite - let's help you to specify it."""
            )

            additional_batch_kwargs = None
            try:
                data_source = select_datasource(context, datasource_name=datasource)
            except ValueError as ve:
                cli_message("<red>{}</red>".format(ve))
                send_usage_message(
                    data_context=context,
                    event="cli.suite.edit",
                    success=False
                )
                sys.exit(1)

            if not data_source:
                cli_message("<red>No datasources found in the context.</red>")
                send_usage_message(
                    data_context=context,
                    event="cli.suite.edit",
                    success=False
                )
                sys.exit(1)

            if batch_kwargs is None:
                (
                    datasource_name,
                    batch_kwargs_generator,
                    data_asset,
                    batch_kwargs,
                ) = get_batch_kwargs(
                    context,
                    datasource_name=data_source.name,
                    batch_kwargs_generator_name=None,
                    generator_asset=None,
                    additional_batch_kwargs=additional_batch_kwargs,
                )

        notebook_name = "{}.ipynb".format(suite.expectation_suite_name)

        notebook_path = os.path.join(
            context.root_directory, context.GE_EDIT_NOTEBOOK_DIR, notebook_name
        )
        NotebookRenderer().render_to_disk(suite, notebook_path, batch_kwargs)

        if not jupyter:
            cli_message("To continue editing this suite, run <green>jupyter "
                        f"notebook {notebook_path}</green>")

        payload = edit_expectation_suite_usage_statistics(
            data_context=context,
            expectation_suite_name=suite.expectation_suite_name
        )

        send_usage_message(
            data_context=context,
            event="cli.suite.edit",
            event_payload=payload,
            success=True
        )

        if jupyter:
            subprocess.call(["jupyter", "notebook", notebook_path])

    except Exception as e:
        send_usage_message(
            data_context=context,
            event="cli.suite.edit",
            success=False
        )
        raise e


@suite.command(name="new")
@click.option("--suite", "-es", default=None, help="Expectation suite name.")
@click.option("--empty", "empty", flag_value=True, help="Create an empty suite.")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--jupyter/--no-jupyter",
    is_flag=True,
    help="By default launch jupyter notebooks unless you specify the --no-jupyter flag",
    default=True,
)
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True,
)
@click.option(
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading the data asset. Must be a valid JSON dictionary",
)
def suite_new(suite, directory, empty, jupyter, view, batch_kwargs):
    """
    Create a new Expectation Suite.

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.

    If you wish to skip the examples, add the `--empty` flag.
    """
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    datasource_name = None
    generator_name = None
    generator_asset = None

    try:
        if batch_kwargs is not None:
            batch_kwargs = json.loads(batch_kwargs)

        success, suite_name = create_expectation_suite_impl(
            context,
            datasource_name=datasource_name,
            batch_kwargs_generator_name=generator_name,
            generator_asset=generator_asset,
            batch_kwargs=batch_kwargs,
            expectation_suite_name=suite,
            additional_batch_kwargs={"limit": 1000},
            empty_suite=empty,
            show_intro_message=False,
            open_docs=view,
        )
        if success:
            cli_message(
                "A new Expectation suite '{}' was added to your project".format(
                    suite_name
                )
            )
            if empty:
                if jupyter:
                    cli_message("""<green>Because you requested an empty suite, we'll open a notebook for you now to edit it!
If you wish to avoid this you can add the `--no-jupyter` flag.</green>\n\n""")
                _suite_edit(suite_name, datasource_name, directory, jupyter=jupyter, batch_kwargs=batch_kwargs)
            send_usage_message(
                data_context=context,
                event="cli.suite.new",
                success=True
            )
        else:
            send_usage_message(
                data_context=context,
                event="cli.suite.new",
                success=False
            )
    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        IOError,
        SQLAlchemyError,
    ) as e:
        cli_message("<red>{}</red>".format(e))
        send_usage_message(
            data_context=context,
            event="cli.suite.new",
            success=False
        )
        sys.exit(1)
    except Exception as e:
        send_usage_message(
            data_context=context,
            event="cli.suite.new",
            success=False
        )
        raise e


@suite.command(name="list")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
def suite_list(directory):
    """Lists available Expectation Suites."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    try:
        suite_names = [
            " - <cyan>{}</cyan>".format(suite_name)
            for suite_name in context.list_expectation_suite_names()
        ]
        if len(suite_names) == 0:
            cli_message("No Expectation Suites found")
            send_usage_message(
                data_context=context,
                event="cli.suite.list",
                success=True
            )
            return

        if len(suite_names) == 1:
            list_intro_string = "1 Expectation Suite found:"

        if len(suite_names) > 1:
            list_intro_string = "{} Expectation Suites found:".format(len(suite_names))

        cli_message_list(suite_names, list_intro_string)
        send_usage_message(
            data_context=context,
            event="cli.suite.list",
            success=True
        )
    except Exception as e:
        send_usage_message(
            data_context=context,
            event="cli.suite.list",
            success=False
        )
        raise e