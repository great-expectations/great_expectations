import logging

import pytest

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.util import filter_properties_dict

yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.fixture()
def checkpoint_dict_reference_for_unexpected_column_names():
    checkpoint_dict: dict = {
        "name": "my_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": None,
        "run_name_template": "%Y-%M-foo-bar-template-test",
        "expectation_suite_name": None,
        "batch_request": None,
        "action_list": [],
        "profilers": [],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_sql_data_connector",
                    "data_asset_name": "my_asset",
                },
                "expectation_suite_name": "animal_names_exp",
            }
        ],
        "runtime_configuration": {},
    }
    return checkpoint_dict


@pytest.fixture()
def checkpoint_dict_unexpected_index_column_names_defined(
    checkpoint_dict_reference_for_unexpected_column_names,
):
    checkpoint_dict = checkpoint_dict_reference_for_unexpected_column_names
    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    return checkpoint_dict


@pytest.fixture()
def checkpoint_dict_unexpected_index_column_names_not_defined(
    checkpoint_dict_reference_for_unexpected_column_names,
):
    checkpoint_dict = checkpoint_dict_reference_for_unexpected_column_names

    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    return checkpoint_dict


@pytest.fixture()
def expectation_config_unexpected_index_column_names_defined():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
                "unexpected_index_column_names": ["pk_1", "pk_2"],  # Single column
            },
        },
    )


@pytest.fixture()
def expectation_config_unexpected_index_column_names_not_defined():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "result_format": "COMPLETE",
            },
        },
    )


def test_result_format_only_in_checkpoint(
    data_context_with_one_expectation,
    checkpoint_dict_unexpected_index_column_names_defined,
    expectation_config_unexpected_index_column_names_not_defined,
):
    context: DataContext = data_context_with_one_expectation
    checkpoint_dict: dict = checkpoint_dict_unexpected_index_column_names_defined
    expectation_configuration: ExpectationConfiguration = (
        expectation_config_unexpected_index_column_names_not_defined
    )

    context.create_expectation_suite(expectation_suite_name="animal_names_exp")
    animals_suite = context.get_expectation_suite(
        expectation_suite_name="animal_names_exp"
    )
    animals_suite.add_expectation(expectation_configuration=expectation_configuration)
    context.save_expectation_suite(
        expectation_suite=animals_suite,
        expectation_suite_name="animal_names_exp",
        overwriting_existing=True,
    )
    checkpoint_config = CheckpointConfig(**checkpoint_dict)
    context.add_checkpoint(
        **filter_properties_dict(
            properties=checkpoint_config.to_json_dict(),
            clean_falsy=True,
        ),
    )
    context._save_project_config()
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs = result.list_validation_results()
    first_result = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_only_in_expectation(
    data_context_with_one_expectation,
    checkpoint_dict_unexpected_index_column_names_not_defined,
    expectation_config_unexpected_index_column_names_defined,
):
    context: DataContext = data_context_with_one_expectation
    checkpoint_dict: dict = checkpoint_dict_unexpected_index_column_names_not_defined
    expectation_configuration: ExpectationConfiguration = (
        expectation_config_unexpected_index_column_names_defined
    )

    context.create_expectation_suite(expectation_suite_name="animal_names_exp")
    animals_suite = context.get_expectation_suite(
        expectation_suite_name="animal_names_exp"
    )
    animals_suite.add_expectation(expectation_configuration=expectation_configuration)
    context.save_expectation_suite(
        expectation_suite=animals_suite,
        expectation_suite_name="animal_names_exp",
        overwriting_existing=True,
    )
    checkpoint_config = CheckpointConfig(**checkpoint_dict)
    context.add_checkpoint(
        **filter_properties_dict(
            properties=checkpoint_config.to_json_dict(),
            clean_falsy=True,
        ),
    )
    context._save_project_config()
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs = result.list_validation_results()
    first_result = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_in_checkpoint_and_expectation_configuration_expectation_ovverides(
    data_context_with_one_expectation,
    checkpoint_dict_unexpected_index_column_names_defined,
    expectation_config_unexpected_index_column_names_defined,
):
    # this one where the expectation configuration overrides the other one
    context: DataContext = data_context_with_one_expectation
    checkpoint_dict: dict = checkpoint_dict_unexpected_index_column_names_defined
    expectation_configuration: ExpectationConfiguration = (
        expectation_config_unexpected_index_column_names_defined
    )

    context.create_expectation_suite(expectation_suite_name="animal_names_exp")
    animals_suite = context.get_expectation_suite(
        expectation_suite_name="animal_names_exp"
    )
    animals_suite.add_expectation(expectation_configuration=expectation_configuration)
    context.save_expectation_suite(
        expectation_suite=animals_suite,
        expectation_suite_name="animal_names_exp",
        overwriting_existing=True,
    )
    checkpoint_config = CheckpointConfig(**checkpoint_dict)
    context.add_checkpoint(
        **filter_properties_dict(
            properties=checkpoint_config.to_json_dict(),
            clean_falsy=True,
        ),
    )
    context._save_project_config()
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs = result.list_validation_results()
    first_result = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result == [
        {"pk_1": 3, "pk_2": "three"},
        {"pk_1": 4, "pk_2": "four"},
        {"pk_1": 5, "pk_2": "five"},
    ]
