import datetime
import json
import sys

import click

from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.datasource import get_batch_kwargs
from great_expectations.cli.mark import Mark as mark
from great_expectations.cli.util import cli_message, cli_message_dict
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.usage_statistics.usage_statistics import send_usage_message

json_parse_exception = json.decoder.JSONDecodeError

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError


@click.group()
def validation_operator():
    """Validation Operator operations"""
    pass


@validation_operator.command(name="list")
@click.option(
    "--directory",
    "-d",
    default=None,
    help="The project's great_expectations directory.",
)
@mark.cli_as_experimental
def validation_operator_list(directory):
    """List known Validation Operators."""
    try:
        context = DataContext(directory)
    except ge_exceptions.ConfigNotFoundError as err:
        cli_message("<red>{}</red>".format(err.message))
        return

    try:
        validation_operators = context.list_validation_operators()

        if len(validation_operators) == 0:
            cli_message("No Validation Operators found")
            return
        elif len(validation_operators) == 1:
            list_intro_string = "1 Validation Operator found:"
        else:
            list_intro_string = "{} Validation Operators found:".format(
                len(validation_operators)
            )

        cli_message(list_intro_string)
        for validation_operator in validation_operators:
            cli_message("")
            cli_message_dict(validation_operator)
        send_usage_message(
            data_context=context, event="cli.validation_operator.list", success=True
        )
    except Exception as e:
        send_usage_message(
            data_context=context, event="cli.validation_operator.list", success=False
        )
        raise e


def _validate_valdiation_config(valdiation_config):
    if "validation_operator_name" not in valdiation_config:
        return "validation_operator_name attribute is missing"
    if "batches" not in valdiation_config:
        return "batches attribute is missing"

    for batch in valdiation_config["batches"]:
        if "batch_kwargs" not in batch:
            return "Each batch must have a BatchKwargs dictionary in batch_kwargs attribute"
        if "expectation_suite_names" not in batch:
            return "Each batch must have a list of expectation suite names in expectation_suite_names attribute"

    return None
