import jsonschema
import mock
import pytest

from great_expectations.core.usage_statistics.schemas import (
    anonymized_batch_schema,
    anonymized_datasource_schema,
    cli_new_ds_choice_payload,
    datasource_sqlalchemy_connect_payload,
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
        "data_context.__init__",\
        "data_context.open_data_docs",\
        "data_context.build_data_docs",\
        "data_context.save_expectation_suite",\
        "data_context.add_datasource",\
        "data_asset.validate",\
        "cli.suite.list",\
        "cli.suite.new",\
        "cli.init.create",\
        "cli.new_ds_choice",\
        "cli.validation_operator.list",\
        "cli.validation_operator.run",\
        "cli.project.check_config",\
        "cli.store.list",\
        "cli.suite.list",\
        "cli.datasource.list",\
        "cli.suite.edit",\
        "cli.datasource.new",\
        "cli.docs.build",\
        "cli.docs.list",\
        "datasource.sqlalchemy.connect"\
    ]
    """


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


def test_data_asset_validate_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_asset.validate"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["data_asset.validate"][0]["event_payload"],
        anonymized_batch_schema,
    )


def test_cli_new_ds_choice_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["data_asset.validate"][0],
        usage_statistics_record_schema,
    )
    # non-empty payload
    jsonschema.validate(
        valid_usage_statistics_messages["cli.new_ds_choice"][0]["event_payload"],
        cli_new_ds_choice_payload,
    )


def test_cli_suite_edit_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["cli.suite.edit"][0],
        usage_statistics_record_schema,
    )
    jsonschema.validate(
        valid_usage_statistics_messages["cli.suite.edit"][0]["event_payload"],
        save_or_edit_expectation_suite_payload_schema,
    )


"""
"datasource.sqlalchemy.connect": [
        {
            "event": "datasource.sqlalchemy.connect",
            "event_payload": {
                "anonymized_name": "6989a7654d0e27470dc01292b6ed0dea",
                "sqlalchemy_dialect": "postgresql",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:38:32.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.5.manual_testing",
        }
    ],
}

"""


def test_datasource_sqlalchemy_connect_message():
    # record itself
    jsonschema.validate(
        valid_usage_statistics_messages["datasource.sqlalchemy.connect"][0],
        usage_statistics_record_schema,
    )
    jsonschema.validate(
        valid_usage_statistics_messages["datasource.sqlalchemy.connect"][0][
            "event_payload"
        ],
        datasource_sqlalchemy_connect_payload,
    )


# all together now
def test_usage_stats_record_schema():
    usage_stats_records_messages = [
        "cli.suite.list",
        "cli.suite.new",
        "cli.store.list",
        "cli.project.check_config",
        "cli.validation_operator.run",
        "cli.validation_operator.list",
        "cli.docs.list",
        "cli.docs.build",
        "cli.datasource.list",
        "cli.datasource.new",
        "data_context.open_data_docs",
        "data_context.build_data_docs",
        "cli.init.create",
    ]
    for message in usage_stats_records_messages:
        jsonschema.validate(
            valid_usage_statistics_messages[message][0], usage_statistics_record_schema,
        )
