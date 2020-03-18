import pytest

from great_expectations.core.logging.usage_statistics import run_validation_operator_usage_statistics
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from tests.integration.usage_statistics.test_integration_usage_statistics import USAGE_STATISTICS_QA_URL


@pytest.fixture
def in_memory_data_context_config():
    return DataContextConfig(**{
        "commented_map": {},
        "config_version": 1,
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
                "action_list": []
            }
        },
        "anonymized_usage_statistics": {
            "enabled": True,
            "data_context_id": "6a52bdfa-e182-455b-a825-e69f076e67d6",
            "usage_statistics_url": USAGE_STATISTICS_QA_URL
        }
    })


def test_consistent_name_anonymization(in_memory_data_context_config):
    context = BaseDataContext(in_memory_data_context_config)
    payload = run_validation_operator_usage_statistics(context, "action_list_operator", assets_to_validate=[(
        {"__fake_batch_kwargs": "mydatasource"}, "__fake_expectation_suite_name")], run_id="foo")
    assert payload["n_assets"] == 1
    # For a *specific* data_context_id, all names will be consistently anonymized
    assert payload["validation_operator_name"] == "9c71a58ca61757cb04b9a2d008416400"
