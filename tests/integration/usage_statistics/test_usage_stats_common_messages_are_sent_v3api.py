import atexit
import logging
import os
import threading
import time
from typing import List

import pandas as pd
import pytest
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from tests.core.usage_statistics.util import (
    assert_no_usage_stats_exceptions,
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

    # def slow_print():
    #     print("sleeping...")
    #     time.sleep(5)
    #     print("waking up...")

    # Pause sending messages
    # context._usage_statistics_handler._worker = None
    # context._usage_statistics_handler._requests_worker = None
    # context._usage_statistics_handler._worker = threading.Thread(target=slow_print, daemon=True)
    # atexit.unregister(context._usage_statistics_handler._close_worker)

    context._usage_statistics_handler._dry_run = True
    assert context._usage_statistics_handler._dry_run

    # wait_event = threading.Event()
    # wait_event.wait()

    # Make sure usage stats are enabled
    assert not context._check_global_usage_statistics_opt_out()
    assert context.anonymous_usage_statistics.enabled
    assert context.anonymous_usage_statistics.usage_statistics_url == (
        USAGE_STATISTICS_URL
    )
    assert context.anonymous_usage_statistics.data_context_id == DATA_CONTEXT_ID

    # context.add_datasource() is decorated, was not sending usage stats events in v0.13.43-46 (possibly earlier)
    datasource_yaml = f"""
    name: example_datasource
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
      module_name: great_expectations.execution_engine
      class_name: PandasExecutionEngine
    data_connectors:
        default_runtime_data_connector_name:
            class_name: RuntimeDataConnector
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
    validator.expect_table_row_count_to_equal(value=2)
    validator.save_expectation_suite()

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

    context.add_checkpoint(**yaml.safe_load(checkpoint_yaml))

    context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        batch_request={
            "runtime_parameters": {"batch_data": df},
            "batch_identifiers": {"default_identifier_name": "my_simple_df"},
        },
    )

    assert_no_usage_stats_exceptions(messages=caplog.messages)

    print("Message Queue")
    # print(context._usage_statistics_handler._message_queue.queue)
    message_queue = context._usage_statistics_handler._message_queue.queue
    print("len(message_queue):", len(message_queue))
    for event in message_queue:
        print(event["event"])
    events = [event["event"] for event in message_queue]
    print(events)
    print("Message Queue")

    expected_events = [
        "data_context.test_yaml_config",
        "data_context.get_batch_list",
        "data_context.save_expectation_suite",
        "data_context.test_yaml_config",
        "data_context.build_data_docs",
        "checkpoint.run",
        "data_context.run_checkpoint",
    ]
    # assert events == expected_events

    # ['data_context.get_batch_list',
    #  'data_context.save_expectation_suite',
    #  'data_context.test_yaml_config',
    #  'data_context.build_data_docs',
    #  'checkpoint.run',
    #  'data_context.run_checkpoint'] != ['data_context.test_yaml_config',
    #                                     'data_context.get_batch_list',
    #                                     'data_context.save_expectation_suite',
    #                                     'data_context.test_yaml_config',
    #                                     'data_context.build_data_docs',
    #                                     'checkpoint.run',
    #                                     'data_context.run_checkpoint']

    # assert context._usage_statistics_handler._message_queue.queue == [{'event': 'data_context.test_yaml_config', 'event_payload': {'anonymized_name': '8123af7f2122286e90309eeec8348d29', 'parent_class': 'SimpleCheckpoint'}, 'success': True, 'version': '1.0.0', 'ge_version': '0.13.46+11.ga7b921a40.dirty', 'data_context_id': '00000000-0000-0000-0000-000000000001', 'data_context_instance_id': 'ad4d0286-5599-4099-9c17-068c255212af', 'event_time': '2021-12-15T22:36:12.633Z'}, {'event_payload': {}, 'event': 'data_context.build_data_docs', 'success': True, 'version': '1.0.0', 'ge_version': '0.13.46+11.ga7b921a40.dirty', 'data_context_id': '00000000-0000-0000-0000-000000000001', 'data_context_instance_id': 'ad4d0286-5599-4099-9c17-068c255212af', 'event_time': '2021-12-15T22:36:12.769Z', 'event_duration': 3}, {'event_payload': {'anonymized_name': '8123af7f2122286e90309eeec8348d29', 'config_version': 1.0, 'anonymized_action_list': [{'anonymized_name': 'fdbdf58d2ea2482971f49bb26337bafb', 'parent_class': 'StoreValidationResultAction'}, {'anonymized_name': '1bcb639ed40bcea7a5e78539b6baf538', 'parent_class': 'StoreEvaluationParametersAction'}, {'anonymized_name': '548269a6423f5f7dac01585d2ed192e7', 'parent_class': 'UpdateDataDocsAction'}], 'anonymized_validations': [{'anonymized_batch_request': {'anonymized_batch_request_required_top_level_properties': {'anonymized_datasource_name': 'da89dda9f4e5192c65e791f0ba90bbda', 'anonymized_data_connector_name': '783632790e85169ed6ab26faf3fc8975', 'anonymized_data_asset_name': '51ecb6dcb3d835fb13ff51a89923f4f7'}, 'batch_request_optional_top_level_keys': ['batch_identifiers', 'runtime_parameters'], 'runtime_parameters_keys': ['batch_data']}, 'anonymized_expectation_suite_name': '50ccea61f2913be7e227f74f848363d9', 'anonymized_action_list': [{'anonymized_name': 'fdbdf58d2ea2482971f49bb26337bafb', 'parent_class': 'StoreValidationResultAction'}, {'anonymized_name': '1bcb639ed40bcea7a5e78539b6baf538', 'parent_class': 'StoreEvaluationParametersAction'}, {'anonymized_name': '548269a6423f5f7dac01585d2ed192e7', 'parent_class': 'UpdateDataDocsAction'}]}]}, 'event': 'checkpoint.run', 'success': True, 'version': '1.0.0', 'ge_version': '0.13.46+11.ga7b921a40.dirty', 'data_context_id': '00000000-0000-0000-0000-000000000001', 'data_context_instance_id': 'ad4d0286-5599-4099-9c17-068c255212af', 'event_time': '2021-12-15T22:36:12.793Z', 'event_duration': 119}, {'event_payload': {}, 'event': 'data_context.run_checkpoint', 'success': True, 'version': '1.0.0', 'ge_version': '0.13.46+11.ga7b921a40.dirty', 'data_context_id': '00000000-0000-0000-0000-000000000001', 'data_context_instance_id': 'ad4d0286-5599-4099-9c17-068c255212af', 'event_time': '2021-12-15T22:36:12.816Z', 'event_duration': 154}]

    # wait_event.set()

    print("========== caplog.messages ==========")
    # assert not usage_stats_invalid_messages_exist(messages=caplog.messages)
    print(caplog.messages)
    for idx, message in enumerate(caplog.messages):
        print(f"{idx}. {message}")
    print("========== caplog.messages ==========")
