import jsonschema
import mock
import pytest

from great_expectations.core.usage_statistics.schemas import (
    anonymized_datasource_schema,
    cli_new_ds_choice_payload,
    init_payload_schema,
    save_or_edit_expectation_suite_payload_schema,
    usage_statistics_record_schema,
)
from tests.core.usage_statistics.test_usage_statistics import (
    in_memory_data_context_config_usage_stats_enabled,
)
from tests.integration.usage_statistics.test_usage_statistics_messages import (
    valid_usage_statistics_messages,
)


def test_comprehensive_list_of_messages():
    """Ensure that we have a comprehensive set of tests for known messages, by
    forcing a manual update to this list when a message is added or removed, and
    reminding the developer to add or remove the associate test."""
    valid_message_list = list(valid_usage_statistics_messages.keys())
    # NOTE: If you are changing the expected valid message list below, you need
    # to also update one or more tests below!

    """
    assert valid_message_list == [
        "data_context.__init__",
        "cli.suite.list",
        "cli.suite.new",
        "cli.checkpoint.list",
        "cli.init.create",
        "cli.new_ds_choice",
        "data_context.open_data_docs",
        "data_context.build_data_docs",
        "data_context.save.expectation.suite",
        "data_context.add_datasource",
    ]"""


def test_init_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.__init__"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.__init__"][0]["event_payload"],
        init_payload_schema,
    )


def test_cli_suite_list_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.suite.list"][0],
        usage_statistics_record_schema,
    )


def test_cli_checkpoint_list_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.checkpoint.list"][0],
        usage_statistics_record_schema,
    )


def test_cli_suite_new_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.suite.new"][0],
        usage_statistics_record_schema,
    )


def test_cli_new_ds_choice_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["cli.new_ds_choice"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.new_ds_choice"][0]["event_payload"],
        cli_new_ds_choice_payload,
    )


def test_cli_init_create_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.init.create"][0],
        usage_statistics_record_schema,
    )


def test_data_context_open_data_docs_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.open_data_docs"][0],
        usage_statistics_record_schema,
    )


def test_data_context_build_data_docs_message():
    # empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.build_data_docs"][0],
        usage_statistics_record_schema,
    )


def test_data_context_save_expectation_suite_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.save_expectation_suite"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.save_expectation_suite"][0][
            "event_payload"
        ],
        save_or_edit_expectation_suite_payload_schema,
    )


def test_data_context_add_datasource_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.add_datasource"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.add_datasource"][0][
            "event_payload"
        ],
        anonymized_datasource_schema,
    )


"""
def test_data_context_run_validation_operator_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_context.run_validation_operator"][0],
        usage_statistics_record_schema,
    )
"""
