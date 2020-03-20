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
    get_batch_kwargs,
    select_datasource,
)
from great_expectations.cli.util import cli_message

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
    help="""Run id. If not specified, a timestamp-based run id will be automatically generated. """,
)
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
def validation_operator_run(name, run_id, validation_config_file, suite, directory):
    """
    Run a validation operator.

    If you are not familiar with validation operators, start here:
    https://great-expectations.readthedocs.io/en/latest/features/validation.html#validation-operators

    There are two modes to run this command:

    1. Interactive:
        Specify the name of the validation operator using the --name argument
        and the name of the expectation suite using the --suite argument.

        The command will help you specify the batch of data that you want the
        validation operator to validate interactively.

    2. Non-interactive:
        Use the `--validation_config_file` argument to specify the path of the validation configuration JSON file.
        The file can be used to instruct a validation operator to validate multiple batches of data and use multiple expectation suites to validate each batch.

        Learn how to create a validation config file here:
        https://great-expectations.readthedocs.io/en/latest/command_line.html#great-expectations-validation-operator-run-validation-config-file-validation-config-file-path
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
        if suite is None:
            cli_message(
"""
Please use --suite argument to specify the name of the expectation suite.
Call `suite list` command to list the expectation suites in your project.
"""
            )
            sys.exit(0)

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

        if name is None:
            cli_message(
"""
Please use --name argument to specify the name of the validation operator.
Call `validation-operator list` command to list the operators in your project.
"""
            )
            sys.exit(0)
        else:
            if name not in context.list_validation_operators():
                cli_message(
"""
Could not find a validation operator {0:s}.
Call `validation-operator list` command to list the operators in your project.
""".format(name)
                )
                sys.exit(0)

        batch_kwargs = None

        cli_message(
            """
Let's help you specify the batch of data your want the validation operator to validate."""
        )

        try:
            data_source = select_datasource(context)
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
                additional_batch_kwargs=None,
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
