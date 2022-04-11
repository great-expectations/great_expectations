import logging
import os
from typing import List

import pandas as pd
import pytest
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)

USAGE_STATISTICS_URL = USAGE_STATISTICS_QA_URL
DATA_CONTEXT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def in_memory_data_context_config_usage_stats_enabled():

    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 3,
            "plugins_directory": None,
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "validations_store",
            "expectations_store_name": "expectations_store",
            "checkpoint_store_name": "checkpoints_store",
            "config_variables_file_path": None,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                },
                "checkpoints_store": {
                    "class_name": "CheckpointStore",
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
            },
            "data_docs_sites": {},
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": DATA_CONTEXT_ID,
                "usage_statistics_url": USAGE_STATISTICS_URL,
            },
        }
    )


def test_common_usage_stats_are_sent_no_mocking(
    caplog, in_memory_data_context_config_usage_stats_enabled, monkeypatch
):
    """
    What does this test and why?
    Our usage stats events are tested elsewhere in several ways (sending example events, validating sample events, throughout other tests ensuring the right events are sent, anonymization, opt-out, etc). This specific test is to ensure that there are no errors with the machinery to send the events in the UsageStatisticsHandler by running code that emits events and checking for errors in the log. This test purposely does not mock any part of the usage stats system to ensure the full code path is run, and sends events to the QA endpoint. This test uses both methods decorated with usage_statistics_enabled_method and those that send events directly.
    """

    # caplog default is WARNING and above, we want to see DEBUG level messages for this test
    caplog.set_level(
        level=logging.DEBUG,
        logger="great_expectations.core.usage_statistics.usage_statistics",
    )

    # Make sure usage stats are enabled
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    assert os.getenv("GE_USAGE_STATS") is None

    context: BaseDataContext = BaseDataContext(
        in_memory_data_context_config_usage_stats_enabled
    )

    # Note, we lose the `data_context.__init__` event because it was emitted before closing the worker
    context._usage_statistics_handler._close_worker()

    # Make sure usage stats are enabled
    assert not context._check_global_usage_statistics_opt_out()
    assert context.anonymous_usage_statistics.enabled
    assert context.anonymous_usage_statistics.data_context_id == DATA_CONTEXT_ID

    # Note module_name fields are omitted purposely to ensure we are still able to send events
    datasource_yaml = """
    name: example_datasource
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
      # module_name: great_expectations.execution_engine
      class_name: PandasExecutionEngine
    data_connectors:
        default_runtime_data_connector_name:
            class_name: RuntimeDataConnector
            # module_name: great_expectations.datasource.data_connector
            batch_identifiers:
                - default_identifier_name
    """

    # context.test_yaml_config() uses send_usage_message()
    context.test_yaml_config(yaml_config=datasource_yaml)
    expected_events: List[str] = ["data_context.test_yaml_config"]

    context.add_datasource(**yaml.load(datasource_yaml))
    expected_events.append("data_context.add_datasource")

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    batch_request = RuntimeBatchRequest(
        datasource_name="example_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",  # This can be anything that identifies this data_asset for you
        runtime_parameters={"batch_data": df},  # df is your dataframe
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    expected_events.append("data_context.get_batch_list")
    validator.expect_table_row_count_to_equal(value=2)
    validator.save_expectation_suite()
    expected_events.append("data_context.save_expectation_suite")

    checkpoint_yaml = """
    name: my_checkpoint
    config_version: 1
    class_name: SimpleCheckpoint
    validations:
      - batch_request:
            datasource_name: example_datasource
            data_connector_name: default_runtime_data_connector_name
            data_asset_name: my_data_asset
        expectation_suite_name: test_suite

    """
    context.test_yaml_config(yaml_config=checkpoint_yaml)
    expected_events.append("data_context.test_yaml_config")

    # Note: add_checkpoint is not instrumented as of 20211215
    context.add_checkpoint(**yaml.safe_load(checkpoint_yaml))

    context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        batch_request={
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {"default_identifier_name": "my_simple_df"},
        },
    )

    expected_events.append("data_context.get_batch_list")
    expected_events.append("data_asset.validate")
    expected_events.append("data_context.build_data_docs")
    expected_events.append("checkpoint.run")
    expected_events.append("data_context.run_checkpoint")

    assert not usage_stats_exceptions_exist(messages=caplog.messages)

    message_queue = context._usage_statistics_handler._message_queue.queue
    events = [event["event"] for event in message_queue]

    # Note: expected events does not contain the `data_context.__init__` event
    assert events == expected_events

    assert not usage_stats_invalid_messages_exist(caplog.messages)
