import json
import os
import subprocess
import sys

import click
from datetime import datetime
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.datasource import (
    create_expectation_suite as create_expectation_suite_impl,
)
from great_expectations.cli.datasource import (
    get_batch_kwargs,
    select_datasource,
)
from great_expectations.cli.util import cli_message
from great_expectations.data_asset import DataAsset
from ruamel.yaml import YAML, YAMLError

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
def validation_operator():
    """validation operator operations"""
    pass


@validation_operator.command(name="run")
@click.option(
    "--validation_config_file",
    "-f",
    default=None,
    help="""The path of the validation config file (JSON). """,
)
@click.option(
    "--name",
    "-n",
    default=None,
    help="""The name of the validation operator. """,
)
@click.option(
    "--suite",
    "-s",
    default=None,
    help="""The name of the expectation suite. """,
)
@click.option(
    "--run_id",
    "-r",
    default=None,
    help="""Run id """,
)
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
def validation_operator_run(name, run_id, validation_config_file, suite, datasource, directory, jupyter, batch_kwargs):
    """
    Run validation operator

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

    if validation_config_file is not None:
        try:
            with open(validation_config_file) as f:
                validation_config = json.load(f)
        except IOError:
            raise ge_exceptions.ConfigNotFoundError()

    else:

        if suite.endswith(".json"):
            suite = suite[:-5]
        try:
            suite = context.get_expectation_suite(suite)
        except ge_exceptions.DataContextError as e:
            cli_message(
                f"<red>Could not find a suite named `{suite_name}`.</red> Please check "
                "the name by running `great_expectations suite list` and try again."
            )
            logger.info(e)
            sys.exit(1)

        batch_kwargs_json = batch_kwargs
        batch_kwargs = None
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
                sys.exit(1)
            except ge_exceptions.DataContextError:
                cli_message(
                    "<red>Please check that your batch_kwargs are able to load a batch.</red>"
                )
                sys.exit(1)
            except ValueError as ve:
                cli_message(
                    "<red>Please check that your batch_kwargs are able to load a batch.\n{}</red>".format(
                        ve
                    )
                )
                sys.exit(1)

        if not batch_kwargs:
            cli_message(
                """
    A batch of data is required - let's help you to specify it."""
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
                (
                    datasource_name,
                    batch_kwarg_generator,
                    data_asset,
                    batch_kwargs,
                ) = get_batch_kwargs(
                    context,
                    datasource_name=data_source.name,
                    generator_name=None,
                    generator_asset=None,
                    additional_batch_kwargs=additional_batch_kwargs,
                )


        validation_config = {
            "validation_operator_name": name,
            "batches": [
                {
                "batch_kwargs": batch_kwargs,
                "expectation_suite_names": [suite.expectation_suite_name]
                }
            ]
        }

    validation_operator_name = validation_config["validation_operator_name"]
    batches_to_validate = []
    for entry in validation_config["batches"]:
        for expectation_suite_name in entry["expectation_suite_names"]:
            batch = context.get_batch(entry["batch_kwargs"], expectation_suite_name)
            batches_to_validate.append(batch)

    if run_id is None:
        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

    results = context.run_validation_operator(
        validation_operator_name,
        assets_to_validate=[batch],
        run_id=run_id)  # e.g., Airflow run id or some run identifier that your pipeline uses.

    if not results["success"]:
        cli_message("Validation Failed!")
        sys.exit(1)
    else:
        cli_message("Validation Succeeded!")
        sys.exit(0)

# @validation_operator.command(name="list")
# @click.option(
#     "--directory",
#     "-d",
#     default=None,
#     help="The project's great_expectations directory.",
# )
# def suite_list(directory):
#     """Lists available expectation suites."""
#     try:
#         context = DataContext(directory)
#     except ge_exceptions.ConfigNotFoundError as err:
#         cli_message("<red>{}</red>".format(err.message))
#         return
#
#     suite_names = context.list_expectation_suite_names()
#     if len(suite_names) == 0:
#         cli_message("No expectation suites found")
#         return
#
#     if len(suite_names) == 1:
#         cli_message("1 expectation suite found:")
#
#     if len(suite_names) > 1:
#         cli_message("{} expectation suites found:".format(len(suite_names)))
#
#     for name in suite_names:
#         cli_message("\t{}".format(name))
