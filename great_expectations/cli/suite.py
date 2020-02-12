import json
import os
import subprocess
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.datasource import (
    create_expectation_suite as create_expectation_suite_impl,
    select_datasource,
    get_batch_kwargs
)
from great_expectations.cli.util import (
    _offer_to_install_new_template,
    cli_message,
)
from great_expectations.data_asset import DataAsset
from great_expectations.render.renderer.notebook_renderer import (
    NotebookRenderer,
)

try:
    json_parse_exception = json.decoder.JSONDecodeError
except AttributeError:  # Python 2
    json_parse_exception = ValueError

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.group()
def suite():
    """expectation suite operations"""
    pass


@suite.command(name="edit")
@click.argument("suite")
@click.option(
    "--datasource",
    "-ds",
    default=None,
    help="""The name of the datasource. The datasource must contain a single BatchKwargGenerator that can list data assets in the datasource """
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
    Generate a Jupyter notebook for editing an existing expectation suite.

    The SUITE argument is required. This is the name you gave to the suite
    when you created it.

    A batch of data is required to edit the suite, which is used as a sample.

    The edit command will help you specify a batch interactively. Or you can
    specify them manually by providing --batch-kwargs in valid JSON format.

    Read more about specifying batches of data in the documentation: https://docs.greatexpectations.io/
    """
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, context.root_directory)
        return

    suite = _load_suite(context, suite)

    if batch_kwargs:
        try:
            batch_kwargs = json.loads(batch_kwargs)
            if datasource:
                batch_kwargs["datasource"] = datasource
            _batch = context.get_batch(batch_kwargs, suite.expectation_suite_name)
            assert isinstance(_batch, DataAsset)
        except json_parse_exception as je:
            cli_message("<red>Please check that your batch_kwargs are valid JSON.\n{}</red>".format(je))
            sys.exit(1)
        except ge_exceptions.DataContextError:
            cli_message("<red>Please check that your batch_kwargs are able to load a batch.</red>")
            sys.exit(1)
        except ValueError as ve:
            cli_message("<red>Please check that your batch_kwargs are able to load a batch.\n{}</red>".format(ve))
            sys.exit(1)
    else:
        cli_message("""
A batch of data is required to edit the suite - let's help you to specify it."""
        )

        additional_batch_kwargs = None
        try:
            data_source = select_datasource(context, datasource_name=datasource)
        except ValueError as ve:
            cli_message("<red>{}</red>".format(ve))
            sys.exit(1)

        if not data_source:
            cli_message("<red>No datasources found in the context.</red>")
            sys.exit(1)

        if batch_kwargs is None:
            datasource_name, batch_kwarg_generator, data_asset, batch_kwargs = get_batch_kwargs(
                context,
                datasource_name=data_source.name,
                generator_name=None,
                generator_asset=None,
                additional_batch_kwargs=additional_batch_kwargs
            )

    notebook_name = "{}.ipynb".format(suite.expectation_suite_name)

    notebook_path = os.path.join(context.root_directory, context.GE_EDIT_NOTEBOOK_DIR, notebook_name)
    NotebookRenderer().render_to_disk(suite, batch_kwargs, notebook_path)

    cli_message(
        "To continue editing this suite, run <green>jupyter notebook {}</green>".format(
            notebook_path
        )
    )

    if jupyter:
        subprocess.call(["jupyter", "notebook", notebook_path])


def _load_suite(context, suite_name):
    if suite_name.endswith(".json"):
        suite_name = suite_name[:-5]
    try:
        suite = context.get_expectation_suite(suite_name)
        return suite
    except ge_exceptions.DataContextError as e:
        cli_message(
            "<red>Could not find a suite named `{}`. Please check the name and try again.</red>".format(
                suite_name
            )
        )
        logger.info(e)
        sys.exit(1)


@suite.command(name="new")
@click.option("--suite", "-es", default=None, help="Expectation suite name.")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@click.option(
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True
)
@click.option(
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading the data asset. Must be a valid JSON dictionary",
)
def suite_new(suite, directory, view, batch_kwargs):
    """
    Create a new expectation suite.

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.
    """
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, context.root_directory)
        return

    if batch_kwargs is not None:
        batch_kwargs = json.loads(batch_kwargs)

    datasource_name = None
    generator_name = None
    generator_asset = None

    try:
        success, suite_name = create_expectation_suite_impl(
            context,
            datasource_name=datasource_name,
            generator_name=generator_name,
            generator_asset=generator_asset,
            batch_kwargs=batch_kwargs,
            expectation_suite_name=suite,
            additional_batch_kwargs={"limit": 1000},
            show_intro_message=False,
            open_docs=view,
        )
        if success:
            cli_message("A new Expectation suite '{}' was added to your project".format(suite_name))
    except (
        ge_exceptions.DataContextError,
        ge_exceptions.ProfilerError,
        IOError,
        SQLAlchemyError
    ) as e:
        cli_message("<red>{}</red>".format(e))
        sys.exit(1)
