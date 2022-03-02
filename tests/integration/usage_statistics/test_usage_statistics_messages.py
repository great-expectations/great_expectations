"""Test usage statistics transmission client-side."""
import copy
from typing import Any, Dict, List

import pytest
import requests

from great_expectations.core.usage_statistics.anonymizers.types.base import (
    GETTING_STARTED_DATASOURCE_NAME,
    CLISuiteInteractiveFlagCombinations,
)
from great_expectations.data_context import BaseDataContext
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)


def generate_messages_with_defaults(
    defaults: Dict[str, Any], message_stubs: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Create a list of messages by overriding defaults with message_stubs
    Args:
        defaults: Dict of default message items
        message_stubs: Unique parts of message

    Returns:
        List of messages same len(message_stubs) combining defaults overridden by message stubs
    """
    output_list = []
    for message_stub in message_stubs:
        defaults_copy = copy.deepcopy(defaults)
        defaults_copy.update(message_stub)
        output_list.append(defaults_copy)

    return output_list


def test_generate_messages_with_defaults():

    defaults = {
        "success": True,
        "version": "1.0.0",
        "event_time": "2020-08-04T22:50:58.837Z",
        "data_context_id": "00000000-0000-0000-0000-000000000002",
        "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
    }

    message_stubs = [
        {
            "event": "cli.checkpoint.new",
            "event_payload": {},
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v2"},
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v3"},
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.new.begin",
            "event_payload": {"api_version": "v3"},
            "ge_version": "0.13.18.manual_testing",
        },
        {
            "event": "cli.checkpoint.new.end",
            "event_payload": {"api_version": "v3"},
            "ge_version": "0.13.18.manual_testing",
        },
    ]

    output = generate_messages_with_defaults(
        defaults=defaults, message_stubs=message_stubs
    )

    expected = [
        {
            "event": "cli.checkpoint.new",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.new",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.checkpoint.new.begin",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.18.manual_testing",
        },
        {
            "event": "cli.checkpoint.new.end",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.18.manual_testing",
        },
    ]

    assert output == expected


"""
valid_usage_statistics_messages should include a list of messages that we want to ensure are valid.

Whenever a new kind of message is added, an example of that message should be included here.

Each message will be sent to the server to ensure it is accepted.

"""
valid_usage_statistics_messages = {
    "data_context.__init__": [
        {
            "event_payload": {
                "platform.system": "Darwin",
                "platform.release": "19.3.0",
                "version_info": "sys.version_info(major=3, minor=7, micro=4, releaselevel='final', serial=0)",
                "anonymized_datasources": [
                    {
                        "anonymized_name": "f57d8a6edae4f321b833384801847498",
                        "parent_class": "SqlAlchemyDatasource",
                        "sqlalchemy_dialect": "postgresql",
                    },
                ],
                "anonymized_stores": [
                    {
                        "anonymized_name": "078eceafc1051edf98ae2f911484c7f7",
                        "parent_class": "ExpectationsStore",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                    },
                    {
                        "anonymized_name": "313cbd9858dd92f3fc2ef1c10ab9c7c8",
                        "parent_class": "ValidationsStore",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                    },
                    {
                        "anonymized_name": "2d487386aa7b39e00ed672739421473f",
                        "parent_class": "EvaluationParameterStore",
                        "anonymized_store_backend": {
                            "parent_class": "InMemoryStoreBackend"
                        },
                    },
                ],
                "anonymized_validation_operators": [
                    {
                        "anonymized_name": "99d14cc00b69317551690fb8a61aca94",
                        "parent_class": "ActionListValidationOperator",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "5a170e5b77c092cc6c9f5cf2b639459a",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "0fffe1906a8f2a5625a5659a848c25a3",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "101c746ab7597e22b94d6e5f10b75916",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
                "anonymized_data_docs_sites": [
                    {
                        "parent_class": "SiteBuilder",
                        "anonymized_name": "eaf0cf17ad63abf1477f7c37ad192700",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                        "anonymized_site_index_builder": {
                            "parent_class": "DefaultSiteIndexBuilder",
                            "show_cta_footer": True,
                        },
                    },
                ],
                "anonymized_expectation_suites": [
                    {
                        "anonymized_name": "238e99998c7674e4ff26a9c529d43da4",
                        "expectation_count": 8,
                        "anonymized_expectation_type_counts": {
                            "expect_column_value_lengths_to_be_between": 1,
                            "expect_table_row_count_to_be_between": 1,
                            "expect_column_values_to_not_be_null": 2,
                            "expect_column_distinct_values_to_be_in_set": 1,
                            "expect_column_kl_divergence_to_be_less_than": 1,
                            "expect_table_column_count_to_equal": 1,
                            "expect_table_columns_to_match_ordered_list": 1,
                        },
                    },
                ],
            },
            "event": "data_context.__init__",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-03-28T01:14:21.155Z",
            "data_context_id": "96c547fe-e809-4f2e-b122-0dc91bb7b3ad",
            "data_context_instance_id": "445a8ad1-2bd0-45ce-bb6b-d066afe996dd",
            "ge_version": "0.11.9.manual_test",
        },
        # "new-style" expectation type system
        {
            "event_payload": {
                "platform.system": "Darwin",
                "platform.release": "19.3.0",
                "version_info": "sys.version_info(major=3, minor=7, micro=4, releaselevel='final', serial=0)",
                "anonymized_datasources": [
                    {
                        "anonymized_name": "f57d8a6edae4f321b833384801847498",
                        "parent_class": "SqlAlchemyDatasource",
                        "sqlalchemy_dialect": "postgresql",
                    },
                ],
                "anonymized_stores": [
                    {
                        "anonymized_name": "078eceafc1051edf98ae2f911484c7f7",
                        "parent_class": "ExpectationsStore",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                    },
                    {
                        "anonymized_name": "313cbd9858dd92f3fc2ef1c10ab9c7c8",
                        "parent_class": "ValidationsStore",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                    },
                    {
                        "anonymized_name": "2d487386aa7b39e00ed672739421473f",
                        "parent_class": "EvaluationParameterStore",
                        "anonymized_store_backend": {
                            "parent_class": "InMemoryStoreBackend"
                        },
                    },
                ],
                "anonymized_validation_operators": [
                    {
                        "anonymized_name": "99d14cc00b69317551690fb8a61aca94",
                        "parent_class": "ActionListValidationOperator",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "5a170e5b77c092cc6c9f5cf2b639459a",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "0fffe1906a8f2a5625a5659a848c25a3",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "101c746ab7597e22b94d6e5f10b75916",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
                "anonymized_data_docs_sites": [
                    {
                        "parent_class": "SiteBuilder",
                        "anonymized_name": "eaf0cf17ad63abf1477f7c37ad192700",
                        "anonymized_store_backend": {
                            "parent_class": "TupleFilesystemStoreBackend"
                        },
                        "anonymized_site_index_builder": {
                            "parent_class": "DefaultSiteIndexBuilder",
                            "show_cta_footer": True,
                        },
                    },
                ],
                "anonymized_expectation_suites": [
                    {
                        "anonymized_name": "238e99998c7674e4ff26a9c529d43da4",
                        "expectation_count": 8,
                        "anonymized_expectation_counts": [
                            {
                                "expectation_type": "expect_column_value_lengths_to_be_between",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_row_count_to_be_between",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_column_values_to_not_be_null",
                                "count": 2,
                            },
                            {
                                "expectation_type": "expect_column_distinct_values_to_be_in_set",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_column_count_to_equal",
                                "count": 1,
                            },
                            {
                                "expectation_type": "expect_table_columns_to_match_ordered_list",
                                "count": 1,
                            },
                        ],
                    },
                ],
            },
            "event": "data_context.__init__",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-03-28T01:14:21.155Z",
            "data_context_id": "96c547fe-e809-4f2e-b122-0dc91bb7b3ad",
            "data_context_instance_id": "445a8ad1-2bd0-45ce-bb6b-d066afe996dd",
            "ge_version": "0.13.0.manual_test",
        },
    ],
    "data_asset.validate": [
        {
            "event": "data_asset.validate",
            "event_payload": {
                "anonymized_batch_kwarg_keys": [
                    "path",
                    "datasource",
                    "data_asset_name",
                ],
                "anonymized_expectation_suite_name": "dbb859464809a03647feb14a514f12b8",
                "anonymized_datasource_name": "a41caeac7edb993cfbe55746e6a328b5",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:36:26.422Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
    ],
    "data_context.add_datasource": [
        {
            "event": "data_context.add_datasource",
            "event_payload": {
                "anonymized_name": "c9633f65c36d1ba9fbaa9009c1404cfa",
                "parent_class": "PandasDatasource",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "data_context.get_batch_list": [
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                    "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                    "anonymized_data_asset_name": "9104abd890c05a364f379443b9f43825",
                },
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                    "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                    "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                },
                "batch_request_optional_top_level_keys": ["data_connector_query"],
                "data_connector_query_keys": ["index"],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                    "anonymized_data_connector_name": "e475f70ca0bcbaf2748b93da5e9867ec",
                    "anonymized_data_asset_name": "2621a5230efeef1973ff373dd12b1ac4",
                },
                "batch_request_optional_top_level_keys": ["batch_spec_passthrough"],
                "batch_spec_passthrough_keys": ["reader_options"],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                    "anonymized_data_connector_name": "e475f70ca0bcbaf2748b93da5e9867ec",
                    "anonymized_data_asset_name": "2621a5230efeef1973ff373dd12b1ac4",
                },
                "batch_request_optional_top_level_keys": [
                    "data_connector_query",
                    "batch_spec_passthrough",
                ],
                "batch_spec_passthrough_keys": ["reader_options"],
                "data_connector_query_keys": ["index"],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                    "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                    "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                },
                "batch_request_optional_top_level_keys": [
                    "data_connector_query",
                    "batch_spec_passthrough",
                ],
                "batch_spec_passthrough_keys": ["reader_method"],
                "data_connector_query_keys": ["index"],
                "runtime_parameters_keys": ["path"],
            },
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                    "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                    "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                },
                "batch_request_optional_top_level_keys": [
                    "batch_spec_passthrough",
                    "data_connector_query",
                ],
                "data_connector_query_keys": ["index"],
                "runtime_parameters_keys": ["path"],
                "batch_spec_passthrough_keys": ["reader_method", "reader_options"],
            },
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:16.030Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                    "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                    "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                },
                "batch_request_optional_top_level_keys": [
                    "batch_identifiers",
                    "runtime_parameters",
                ],
                "runtime_parameters_keys": ["batch_data"],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.get_batch_list",
            "event_payload": {
                "anonymized_batch_request_required_top_level_properties": {
                    "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                    "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                    "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                },
                "batch_request_optional_top_level_keys": [
                    "batch_identifiers",
                    "runtime_parameters",
                ],
                "runtime_parameters_keys": ["batch_data"],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "data_context.build_data_docs": [
        {
            "event_payload": {},
            "event": "data_context.build_data_docs",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:24.349Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "data_context.open_data_docs": [
        {
            "event_payload": {},
            "event": "data_context.open_data_docs",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "data_context.run_checkpoint": [
        {
            "event": "data_context.run_checkpoint",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "data_context.run_checkpoint",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "checkpoint.run": [
        {
            "event": "checkpoint.run",
            "event_payload": {},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 1,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "48533197103a407af37326b0224a97df",
                "config_version": 1,
                "anonymized_run_name_template": "21e9677f05fd2b0d83bb9285a688d5c5",
                "anonymized_expectation_suite_name": "4987b41d9e7012f6a86a8b3939739eff",
                "anonymized_action_list": [
                    {
                        "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                        "parent_class": "StoreValidationResultAction",
                    },
                    {
                        "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                        "parent_class": "StoreEvaluationParametersAction",
                    },
                    {
                        "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                        "parent_class": "UpdateDataDocsAction",
                    },
                ],
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "41cc60fba42f099f878a4bb295dc08c9",
                                "anonymized_data_connector_name": "4cffb49069fa5fececc8032aa41ff791",
                                "anonymized_data_asset_name": "5dce9f4b8abd8adbb4f719e05fceecab",
                            },
                            "batch_request_optional_top_level_keys": [
                                "data_connector_query"
                            ],
                        },
                        "anonymized_expectation_suite_name": "4987b41d9e7012f6a86a8b3939739eff",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    }
                ],
                "checkpoint_optional_top_level_keys": [
                    "evaluation_parameters",
                    "runtime_configuration",
                ],
            },
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "eb2d802f924a3e764afc605de3495c5c",
                "config_version": 1.0,
                "anonymized_run_name_template": "21e9677f05fd2b0d83bb9285a688d5c5",
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                                "anonymized_data_connector_name": "e475f70ca0bcbaf2748b93da5e9867ec",
                                "anonymized_data_asset_name": "2621a5230efeef1973ff373dd12b1ac4",
                            },
                            "batch_request_optional_top_level_keys": [
                                "data_connector_query"
                            ],
                            "data_connector_query_keys": ["index"],
                        },
                        "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    }
                ],
                "checkpoint_optional_top_level_keys": [
                    "evaluation_parameters",
                    "runtime_configuration",
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "eb2d802f924a3e764afc605de3495c5c",
                "config_version": 1.0,
                "anonymized_run_name_template": "21e9677f05fd2b0d83bb9285a688d5c5",
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "getting_started_datasource",
                                "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                                "anonymized_data_asset_name": "9104abd890c05a364f379443b9f43825",
                            },
                            "batch_request_optional_top_level_keys": [
                                "batch_spec_passthrough",
                                "data_connector_query",
                            ],
                            "batch_spec_passthrough_keys": ["reader_options"],
                        },
                        "anonymized_expectation_suite_name": "35af1ba156bfe672f8845cb60554b138",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    }
                ],
                "checkpoint_optional_top_level_keys": [
                    "evaluation_parameters",
                    "runtime_configuration",
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "f563d9aa1604e16099bb7dff7b203319",
                "config_version": 1.0,
                "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                "anonymized_action_list": [
                    {
                        "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                        "parent_class": "StoreValidationResultAction",
                    },
                    {
                        "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                        "parent_class": "StoreEvaluationParametersAction",
                    },
                    {
                        "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                        "parent_class": "UpdateDataDocsAction",
                    },
                ],
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                                "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                                "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                            },
                            "batch_request_optional_top_level_keys": [
                                "batch_identifiers",
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["batch_data"],
                        },
                        "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "f563d9aa1604e16099bb7dff7b203319",
                "config_version": 1.0,
                "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                "anonymized_action_list": [
                    {
                        "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                        "parent_class": "StoreValidationResultAction",
                    },
                    {
                        "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                        "parent_class": "StoreEvaluationParametersAction",
                    },
                    {
                        "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                        "parent_class": "UpdateDataDocsAction",
                    },
                ],
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                                "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                                "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                            },
                            "batch_request_optional_top_level_keys": [
                                "batch_identifiers",
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["batch_data"],
                        },
                        "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "d7e22c0913c0cb83d528d2a7ad097f2b",
                "config_version": 1.0,
                "anonymized_run_name_template": "131f67e5ea07d59f2bc5376234f7f9f2",
                "anonymized_expectation_suite_name": "295722d0683963209e24034a79235ba6",
                "anonymized_action_list": [
                    {
                        "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                        "parent_class": "StoreValidationResultAction",
                    },
                    {
                        "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                        "parent_class": "StoreEvaluationParametersAction",
                    },
                    {
                        "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                        "parent_class": "UpdateDataDocsAction",
                    },
                ],
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                                "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                                "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                            },
                            "batch_request_optional_top_level_keys": [
                                "batch_identifiers",
                                "data_connector_query",
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["batch_data"],
                        },
                        "anonymized_expectation_suite_name": "295722d0683963209e24034a79235ba6",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                                "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                                "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                            },
                            "batch_request_optional_top_level_keys": [
                                "batch_identifiers",
                                "data_connector_query",
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["batch_data"],
                        },
                        "anonymized_expectation_suite_name": "295722d0683963209e24034a79235ba6",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
            },
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "f563d9aa1604e16099bb7dff7b203319",
                "config_version": 1.0,
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                                "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                                "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                            },
                            "batch_request_optional_top_level_keys": [
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["query"],
                        },
                        "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "checkpoint.run",
            "event_payload": {
                "anonymized_name": "f563d9aa1604e16099bb7dff7b203319",
                "config_version": 1.0,
                "anonymized_validations": [
                    {
                        "anonymized_batch_request": {
                            "anonymized_batch_request_required_top_level_properties": {
                                "anonymized_datasource_name": GETTING_STARTED_DATASOURCE_NAME,
                                "anonymized_data_connector_name": "d52d7bff3226a7f94dd3510c1040de78",
                                "anonymized_data_asset_name": "556e8c06239d09fc66f424eabb9ca491",
                            },
                            "batch_request_optional_top_level_keys": [
                                "runtime_parameters",
                            ],
                            "runtime_parameters_keys": ["path"],
                        },
                        "anonymized_expectation_suite_name": "6a04fc37da0d43a4c21429f6788d2cff",
                        "anonymized_action_list": [
                            {
                                "anonymized_name": "8e3e134cd0402c3970a02f40d2edfc26",
                                "parent_class": "StoreValidationResultAction",
                            },
                            {
                                "anonymized_name": "40e24f0c6b04b6d4657147990d6f39bd",
                                "parent_class": "StoreEvaluationParametersAction",
                            },
                            {
                                "anonymized_name": "2b99b6b280b8a6ad1176f37580a16411",
                                "parent_class": "UpdateDataDocsAction",
                            },
                        ],
                    },
                ],
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "legacy_profiler.build_suite": [
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Dataset",
                "excluded_expectations_specified": False,
                "ignored_columns_specified": True,
                "not_null_only": False,
                "primary_or_compound_key_specified": True,
                "semantic_types_dict_specified": False,
                "table_expectations_only": False,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Batch",
                "excluded_expectations_specified": True,
                "ignored_columns_specified": False,
                "not_null_only": False,
                "primary_or_compound_key_specified": False,
                "semantic_types_dict_specified": False,
                "table_expectations_only": True,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": False,
                "ignored_columns_specified": True,
                "not_null_only": True,
                "primary_or_compound_key_specified": False,
                "semantic_types_dict_specified": True,
                "table_expectations_only": False,
                "value_set_threshold_specified": False,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": False,
                "ignored_columns_specified": False,
                "not_null_only": False,
                "primary_or_compound_key_specified": False,
                "semantic_types_dict_specified": False,
                "table_expectations_only": False,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": True,
                "ignored_columns_specified": True,
                "not_null_only": False,
                "primary_or_compound_key_specified": True,
                "semantic_types_dict_specified": False,
                "table_expectations_only": False,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": True,
                "ignored_columns_specified": True,
                "not_null_only": False,
                "primary_or_compound_key_specified": True,
                "semantic_types_dict_specified": True,
                "table_expectations_only": False,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": True,
                "ignored_columns_specified": True,
                "not_null_only": False,
                "primary_or_compound_key_specified": False,
                "semantic_types_dict_specified": False,
                "table_expectations_only": True,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "legacy_profiler.build_suite",
            "event_payload": {
                "profile_dataset_type": "Validator",
                "excluded_expectations_specified": True,
                "ignored_columns_specified": True,
                "not_null_only": False,
                "primary_or_compound_key_specified": False,
                "semantic_types_dict_specified": False,
                "table_expectations_only": True,
                "value_set_threshold_specified": True,
                "api_version": "v2",
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.45.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    "data_context.save_expectation_suite": [
        {
            "event": "data_context.save_expectation_suite",
            "event_payload": {
                "anonymized_expectation_suite_name": "4b6bf73298fcc2db6da929a8f18173f7"
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:23.570Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    # BaseDataContext.test_yaml_config() MESSAGES
    "data_context.test_yaml_config": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2021-06-18T14:36:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": class_name,
                    "diagnostic_info": [],
                },
                "ge_version": "0.13.20.manual_testing",
            }
            for class_name in BaseDataContext.ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES
        ]
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": False,
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": class_name,
                    "diagnostic_info": [],
                },
                "ge_version": "0.13.20.manual_testing",
            }
            for class_name in BaseDataContext.ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES
        ]
        # Diagnostic Message Types
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": False,
                "event_payload": {
                    "diagnostic_info": ["__substitution_error__"],
                },
                "ge_version": "0.13.20.manual_testing",
            },
            {
                "event": "data_context.test_yaml_config",
                "success": False,
                "event_payload": {
                    "diagnostic_info": ["__yaml_parse_error__"],
                },
                "ge_version": "0.13.20.manual_testing",
            },
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "diagnostic_info": ["__custom_subclass_not_core_ge__"],
                },
                "ge_version": "0.13.20.manual_testing",
            },
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "diagnostic_info": ["__class_name_not_provided__"],
                },
                "ge_version": "0.13.20.manual_testing",
            },
            {
                "event": "data_context.test_yaml_config",
                "success": False,
                "event_payload": {
                    "diagnostic_info": ["__class_name_not_provided__"],
                },
                "ge_version": "0.13.20.manual_testing",
            },
        ]
        # Store Message Types
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": "ExpectationsStore",
                    "anonymized_store_backend": {
                        "parent_class": "InMemoryStoreBackend"
                    },
                },
                "ge_version": "0.13.20.manual_testing",
            },
        ]
        # Datasource Message Types
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": "Datasource",
                    "anonymized_execution_engine": {
                        "anonymized_name": "fake_anonymized_name_for_testing",
                        "parent_class": "PandasExecutionEngine",
                    },
                    "anonymized_data_connectors": [
                        {
                            "anonymized_name": "fake_anonymized_name_for_testing",
                            "parent_class": "InferredAssetFilesystemDataConnector",
                        },
                    ],
                },
                "ge_version": "0.13.20.manual_testing",
            },
        ]
        # DataConnector Message Types
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": "ConfiguredAssetFilesystemDataConnector",
                },
                "ge_version": "0.13.20.manual_testing",
            },
        ]
        # Checkpoint Message Types
        + [
            {
                "event": "data_context.test_yaml_config",
                "success": True,
                "event_payload": {
                    "anonymized_name": "fake_anonymized_name_for_testing",
                    "parent_class": "Checkpoint",
                },
                "ge_version": "0.13.20.manual_testing",
            },
        ],
    ),
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
        },
    ],
    "expectation_suite.add_expectation": [
        {
            "event_payload": {},
            "event": "expectation_suite.add_expectation",
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.47.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        }
    ],
    # CLI INIT COMMANDS
    "cli.init.create": [
        {
            "event": "cli.init.create",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.init.create",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.init.create",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:06:47.697Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    # CLI PROJECT COMMANDS
    "cli.project.check_config": [
        {
            "event": "cli.project.check_config",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.project.check_config",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.project.check_config",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:42:34.068Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.project.upgrade": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:20:37.828Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.project.upgrade.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.project.upgrade.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.project.upgrade.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    # CLI STORE COMMANDS
    "cli.store.list": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:56:53.908Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.store.list",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.store.list",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.store.list",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.store.list.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.store.list.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.store.list.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    # CLI DATASOURCE COMMANDS
    "cli.datasource.list": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.datasource.list",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.datasource.list",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.list",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.list.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.list.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.list.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.datasource.new": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.datasource.new",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.datasource.new",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.new",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.new.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.new.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.new.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.datasource.delete": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.datasource.delete",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.datasource.delete",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.delete",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.datasource.delete.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.delete.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"api_version": "v3", "cancelled": True},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.datasource.profile": [
        {
            "event": "cli.datasource.profile",
            "event_payload": {},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.datasource.profile",
            "event_payload": {"api_version": "v2"},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.datasource.profile",
            "event_payload": {"api_version": "v3"},
            "success": False,
            "version": "1.0.0",
            "event_time": "2020-08-05T01:03:17.567Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    # CLI NEW_DS_CHOICE COMMANDS
    "cli.new_ds_choice": [
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas", "api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "cli.new_ds_choice",
            "event_payload": {"type": "pandas", "api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:08.963Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
    # CLI SUITE COMMANDS
    "cli.suite.demo": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.suite.demo",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.suite.demo",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.demo",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.demo.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.demo.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.demo.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.suite.list": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.suite.list",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.suite.list",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.list",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.list.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.list.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.list.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.suite.new": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.suite.new",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e"
                },
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.suite.new",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v2",
                },
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.new",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.new.begin",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.new.end",
                "success": False,
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    **CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE_PROFILE_TRUE.value,
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.suite.edit": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.suite.edit",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e"
                },
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.suite.edit",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v2",
                },
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.edit",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.edit.end",
                "success": False,
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "anonymized_expectation_suite_name": "0604e6a8f5a1da77e0438aa3b543846e",
                    **CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_DEFAULT.value,
                    "api_version": "v3",
                },
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.suite.delete": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.suite.delete",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.suite.delete.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.delete.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.delete.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.suite.delete.end",
                "event_payload": {"api_version": "v3", "cancelled": True},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.suite.scaffold": [
        {
            "event": "cli.suite.scaffold",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.suite.scaffold",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
        {
            "event": "cli.suite.scaffold",
            "event_payload": {"api_version": "v3"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:58:51.961Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    # CLI CHECKPOINT COMMANDS
    "cli.checkpoint.new": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.checkpoint.new",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.checkpoint.new",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.new",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.new.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.new.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.new.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.checkpoint.script": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.checkpoint.script",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.checkpoint.script",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.script",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.script.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.script.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.script.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.checkpoint.run": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.checkpoint.run",
                "event_payload": {},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.run",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.run.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.run.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.run.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.checkpoint.list": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.checkpoint.list",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.checkpoint.list",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.list",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.list.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.list.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.list.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.checkpoint.delete": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T22:50:58.837Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.checkpoint.delete.end",
                "event_payload": {"api_version": "v3", "cancelled": True},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    # CLI VALIDATION_OPERATOR COMMANDS
    "cli.validation_operator.list": [
        {
            "event": "cli.validation_operator.list",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:32:33.635Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.validation_operator.list",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:32:33.635Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    "cli.validation_operator.run": [
        {
            "event": "cli.validation_operator.run",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:33:15.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.11.9.manual_testing",
        },
        {
            "event": "cli.validation_operator.run",
            "event_payload": {"api_version": "v2"},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-03T23:33:15.664Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.13.0.manual_testing",
        },
    ],
    # CLI DOCS COMMANDS
    "cli.docs.build": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:25:27.088Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.docs.build",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.docs.build",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.docs.build",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.docs.build.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.build.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.build.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.build.end",
                "event_payload": {"api_version": "v3", "cancelled": True},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.docs.clean": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-05T00:36:50.979Z",
            "data_context_id": "2a948908-ec42-47f2-b972-c07bb0393de4",
            "data_context_instance_id": "e7e0916d-d527-437a-b89d-5eb8c36d408f",
        },
        message_stubs=[
            {
                "event": "cli.docs.clean",
                "event_payload": {},
                "ge_version": "0.11.9+25.g3ca555c.dirty",
            },
            {
                "event": "cli.docs.clean",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0+25.g3ca555c.dirty",
            },
            {
                "event": "cli.docs.clean",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0+25.g3ca555c.dirty",
            },
            {
                "event": "cli.docs.clean.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.clean.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.clean.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.clean.end",
                "event_payload": {"api_version": "v3", "cancelled": True},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "cli.docs.list": generate_messages_with_defaults(
        defaults={
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-08-04T00:20:37.828Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        },
        message_stubs=[
            {
                "event": "cli.docs.list",
                "event_payload": {},
                "ge_version": "0.11.9.manual_testing",
            },
            {
                "event": "cli.docs.list",
                "event_payload": {"api_version": "v2"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.docs.list",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.0.manual_testing",
            },
            {
                "event": "cli.docs.list.begin",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.list.end",
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
            {
                "event": "cli.docs.list.end",
                "success": False,
                "event_payload": {"api_version": "v3"},
                "ge_version": "0.13.18.manual_testing",
            },
        ],
    ),
    "profiler.run": [
        {
            "event": "profiler.run",
            "event_payload": {},
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.14.7.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "profiler.run",
            "event_payload": {
                "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
                "anonymized_rules": [
                    {
                        "anonymized_domain_builder": {
                            "parent_class": "TableDomainBuilder"
                        },
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                            }
                        ],
                        "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                        "anonymized_parameter_builders": [
                            {
                                "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                                "anonymized_class": "dasfj238fefasfa90sdf23j39202f2j2",
                                "parent_class": "MetricMultiBatchParameterBuilder",
                            }
                        ],
                    },
                    {
                        "anonymized_domain_builder": {
                            "parent_class": "TableDomainBuilder"
                        },
                        "anonymized_expectation_configuration_builders": [
                            {
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                                "anonymized_expectation_type": "239asdfjaew832hg20sdjwd9922e9e2u",
                            }
                        ],
                        "anonymized_name": "0bac2cecbb0cf8bb704e86710941434e",
                        "anonymized_parameter_builders": [
                            {
                                "anonymized_name": "b7719efec76c6ebe30230fc1ec023beb",
                                "parent_class": "MetricMultiBatchParameterBuilder",
                            }
                        ],
                    },
                ],
                "config_version": 1.0,
                "rule_count": 2,
                "variable_count": 1,
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.14.7.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
        {
            "event": "profiler.run",
            "event_payload": {
                "anonymized_name": "5b6c98e19e21e77191fb071bb9e80070",
                "anonymized_rules": [
                    {
                        "anonymized_domain_builder": {
                            "anonymized_class": "d2972bccf7a2a0ff91ba9369a86dcbe1",
                            "parent_class": "TableDomainBuilder",
                        },
                        "anonymized_expectation_configuration_builders": [
                            {
                                "anonymized_class": "0d70a2037f19cf1764afad97c7395167",
                                "anonymized_expectation_type": "c7c23fbf56041786bf024a2407031b27",
                                "parent_class": "DefaultExpectationConfigurationBuilder",
                            }
                        ],
                        "anonymized_name": "5a83f3728393d6519a197cffdccd50ff",
                        "anonymized_parameter_builders": [
                            {
                                "anonymized_class": "c73849d7016ce7ab68e24465361a717a",
                                "anonymized_name": "9349ed253aba01f4ecf190af61018a11",
                                "parent_class": "RegexPatternStringParameterBuilder",
                            }
                        ],
                    },
                    {
                        "anonymized_domain_builder": {
                            "anonymized_class": "df79fd715bf3ea514c3f4e3006025b24",
                            "parent_class": "__not_recognized__",
                        },
                        "anonymized_expectation_configuration_builders": [
                            {
                                "anonymized_class": "71128204dee66972b5cfc8851b216508",
                                "anonymized_expectation_type": "828b498d29af626836697ba1622ca234",
                                "parent_class": "__not_recognized__",
                            }
                        ],
                        "anonymized_name": "0bac2cecbb0cf8bb704e86710941434e",
                        "anonymized_parameter_builders": [
                            {
                                "anonymized_class": "fcbd493bd096d894cf83506bc23a0729",
                                "anonymized_name": "5af4c6b6dedc5f9b3b840709e957c4ed",
                                "parent_class": "__not_recognized__",
                            }
                        ],
                    },
                ],
                "config_version": 1.0,
                "rule_count": 2,
                "variable_count": 1,
            },
            "success": True,
            "version": "1.0.0",
            "event_time": "2020-06-25T16:08:28.070Z",
            "event_duration": 123,
            "data_context_id": "00000000-0000-0000-0000-000000000002",
            "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
            "ge_version": "0.14.7.manual_testing",
            "x-forwarded-for": "00.000.00.000, 00.000.000.000",
        },
    ],
}

test_messages = []
message_test_ids = []
for message_type, messages in valid_usage_statistics_messages.items():
    for idx, test_message in enumerate(messages):
        test_messages += [test_message]
        message_test_ids += [f"{message_type}_{idx}"]


@pytest.mark.aws_integration
@pytest.mark.parametrize("message", test_messages, ids=message_test_ids)
def test_usage_statistics_message(message):
    """known message formats should be valid"""
    res = requests.post(USAGE_STATISTICS_QA_URL, json=message, timeout=2)
    assert res.status_code == 201
    assert res.json() == {"event_count": 1}
