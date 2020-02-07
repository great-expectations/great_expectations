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
from great_expectations.datasource.generator import ManualGenerator
from great_expectations.render.renderer.notebook_renderer import (
    NotebookRenderer,
)

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
    "--datasource_name",
    "-ds",
    default=None,
    help="""The name of the datasource. The generator will list data assets in the datasource 
(for getting a batch of data to be used a sample when editing the suite)"""
)
@click.option(
    "--generator_name",
    "-g",
    default=None,
    help="""The name of the batch kwarg generator configured in the datasource. The generator will list data assets in the datasource 
(for getting a batch of data to be used a sample when editing the suite)"""
)
@click.option(
    "--generator_asset",
    "-a",
    default=None,
    help="""The name of data asset. Must be a name listed by the generator  
(for getting a batch of data to be used a sample when editing the suite)"""
)
@click.option(
    "--batch_kwargs",
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
def suite_edit(suite, datasource_name, generator_name, generator_asset, directory, jupyter, batch_kwargs):
    """
    Generate a Jupyter notebook for editing an existing suite.

    SUITE argument is required. This is the name you gave to the suite
    when you created it.

    A batch of data is required to edit the suite. It is used as a sample.

    You can specify it by providing
    * --datasource_name, --generator_name and  --generator_asset arguments
    or
    * --batch_kwargs argument.

    If you do not specify the batch using these arguments, the edit command
    will help you specify a batch.

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

    if suite.endswith(".json"):
        suite = suite[:-5]
    suite = _load_suite(context, suite)

    if batch_kwargs:
        batch_kwargs = json.loads(batch_kwargs)
    else:
        cli_message(
            """
A batch of data is required to edit the suite - let's help you to specify it."""
        )

        additional_batch_kwargs = None
        data_source = select_datasource(context, datasource_name=datasource_name)
        if data_source is None:
            raise ge_exceptions.DataContextError("No datasources found in the context")

        datasource_name = data_source.name

        if generator_name is None or generator_asset is None or batch_kwargs is None:
            datasource_name, generator_name, generator_asset, batch_kwargs = get_batch_kwargs(context,
                                                                                               datasource_name=datasource_name,
                                                                                               generator_name=generator_name,
                                                                                               generator_asset=generator_asset,
                                                                                               additional_batch_kwargs=additional_batch_kwargs)

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
    try:
        suite = context.get_expectation_suite(suite_name)
    except ge_exceptions.DataContextError as e:
        cli_message(
            "<red>Could not find a suite named `{}`. Please check the name and try again.</red>".format(
                suite_name
            )
        )
        logger.info(e)
        sys.exit(1)
    return suite


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
    "--batch_kwargs",
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
