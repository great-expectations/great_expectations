import pytest

from great_expectations.data_context.types.base import DataContextConfig
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)


@pytest.fixture
def in_memory_data_context_config_usage_stats_enabled():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": None,
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "validations_store",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": None,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
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
                "data_context_id": "00000000-0000-0000-0000-000000000001",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
        }
    )


@pytest.fixture
def sample_partial_message():
    return {
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
        # "version": "1.0.0",
        # "event_time": "2020-06-25T16:08:28.070Z",
        # "event_duration": 123,
        # "data_context_id": "00000000-0000-0000-0000-000000000002",
        # "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        # "ge_version": "0.13.45.manual_testing",
        "x-forwarded-for": "00.000.00.000, 00.000.000.000",
    }
