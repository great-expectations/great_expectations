import json
import os
import subprocess
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.datasource import \
    create_expectation_suite as create_expectation_suite_impl
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
    "--batch-kwargs",
    default=None,
    help="Additional keyword arguments to be provided to get_batch when loading \
the data asset. Must be a valid JSON dictionary",
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
def suite_edit(suite, directory, jupyter, batch_kwargs):
    """Edit an existing suite with a jupyter notebook."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return
    except ge_exceptions.ZeroDotSevenConfigVersionError as err:
        _offer_to_install_new_template(err, context.root_directory)
        return

    suite = suite.rstrip(".json")
    suite = _load_suite(context, suite)

    if batch_kwargs:
        batch_kwargs = json.loads(batch_kwargs)
    # elif suite.get_original_batch_kwargs():
    # TODO this functionality doesn't actually exist yet
    # batch_kwargs = suite.get_original_batch_kwargs()
    else:
        cli_message(
            "<red>Unable to identify a batch of data to use to edit the suite; add batch_kwargs.</red>"
        )

    notebook_name = "{}.ipynb".format(suite.expectation_suite_name)

    notebook_path = os.path.join(context.GE_EDIT_NOTEBOOK_DIR, notebook_name)
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
            "<red>Could not locate a suite named {}</red>".format(
                suite_name
            )
        )
        logger.info(e)
        sys.exit(-1)
    return suite


@suite.command(name="new")
@click.option(
    "--data_asset",
    "-da",
    default=None,
    help="Fully qualified data asset name (datasource/generator/generator_asset)",
)
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
def suite_new(data_asset, suite, directory, view, batch_kwargs):
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
    if data_asset is not None:
        try:
            data_asset_id = context.normalize_data_asset_name(data_asset)
            datasource_name = data_asset_id.datasource
            generator_name = data_asset_id.generator
            generator_asset = data_asset_id.generator_asset
        except ge_exceptions.AmbiguousDataAssetNameError as e:
            cli_message(
                """<cyan>Your data_asset name of {} was not specific enough.
    No worries - we will help you to specify the data asset name
      - '{}""".format(
                    data_asset
                )
                + "'</cyan>"
            )

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
