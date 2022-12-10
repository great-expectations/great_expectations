import logging
from typing import List

import pytest

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.util import filter_properties_dict

yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.fixture()
def reference_checkpoint_config_for_unexpected_column_names() -> dict:
    """
    This is a reference checkpoint dict. It is not used by the tests on its own but subsequent functions will add
    runtime_configurations that will then be tested.

    checkpoint_dict_unexpected_index_column_names_defined_one_column()
        - adds runtime_configuration where "unexpected_index_column_names": ["pk_1"],
    checkpoint_dict_unexpected_index_column_names_defined_two_columns()
        - adds runtime_configuration where "unexpected_index_column_names": ["pk_1", "pk_2"],
    checkpoint_dict_unexpected_index_column_names_not_defined()
        - adds runtime_configuration where "unexpected_index_column_names" are defined

    For more information, look at the docstring for data_context_with_connection_to_animal_names_db() fixture

    """
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
def checkpoint_dict_unexpected_index_column_names_defined_complete(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    checkpoint_dict = reference_checkpoint_config_for_unexpected_column_names
    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    return checkpoint_dict


@pytest.fixture()
def checkpoint_dict_unexpected_index_column_names_defined_summary(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    checkpoint_dict = reference_checkpoint_config_for_unexpected_column_names
    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "SUMMARY",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    return checkpoint_dict


@pytest.fixture()
def checkpoint_dict_unexpected_index_column_names_defined_basic(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    checkpoint_dict = reference_checkpoint_config_for_unexpected_column_names
    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "BASIC",
            "unexpected_index_column_names": ["pk_1"],
        }
    }
    return checkpoint_dict


@pytest.fixture()
def checkpoint_dict_unexpected_index_column_names_not_defined(
    reference_checkpoint_config_for_unexpected_column_names,
) -> dict:
    checkpoint_dict = reference_checkpoint_config_for_unexpected_column_names
    checkpoint_dict["runtime_configuration"] = {
        "result_format": {
            "result_format": "COMPLETE",
        }
    }
    return checkpoint_dict


@pytest.fixture()
def expectation_config_expect_column_values_to_be_in_set() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
        },
    )


@pytest.fixture()
def expectation_config_expect_column_values_to_not_be_in_set() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["giraffe", "lion", "zebra"],
        },
    )


@pytest.fixture()
def expectation_config_expect_column_values_to_not_be_in_set_two_columns_defined() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "unexpected_index_column_names": ["pk_1", "pk_2"],
            },
        },
    )


@pytest.fixture()
def expectation_config_expect_column_values_to_be_in_set_one_column_defined() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "animals",
            "value_set": ["cat", "fish", "dog"],
            "result_format": {
                "unexpected_index_column_names": ["pk_2"],
            },
        },
    )


def _add_expectations_and_checkpoint(
    data_context: DataContext,
    checkpoint_config: dict,
    expectations_list: List[ExpectationConfiguration],
) -> DataContext:
    """
    Helper method for adding Checkpoint and Expectations to DataContext.

    Args:
        data_context (DataContext): data_context_with_connection_to_animal_names_db
        checkpoint_config : Checkpoint to add
        expectations_list : Expectations to add

    Returns:
        DataContext with updated config
    """

    context: DataContext = data_context
    context.create_expectation_suite(expectation_suite_name="animal_names_exp")
    animals_suite = context.get_expectation_suite(
        expectation_suite_name="animal_names_exp"
    )
    for expectation in expectations_list:
        animals_suite.add_expectation(expectation_configuration=expectation)
    context.save_expectation_suite(
        expectation_suite=animals_suite,
        expectation_suite_name="animal_names_exp",
        overwriting_existing=True,
    )
    checkpoint_config = CheckpointConfig(**checkpoint_config)
    context.add_checkpoint(
        **filter_properties_dict(
            properties=checkpoint_config.to_json_dict(),
            clean_falsy=True,
        ),
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


def test_result_format_in_checkpoint_pk_defined_one_expectation_complete_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_complete,
    expectation_config_expect_column_values_to_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 1 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_complete,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_in_checkpoint_pk_defined_two_expectation_complete_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_complete,
    expectation_config_expect_column_values_to_be_in_set,
    expectation_config_expect_column_values_to_not_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`
        - 2 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_complete,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result_full_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]

    second_result_full_list = evrs[0]["results"][1]["result"]["unexpected_index_list"]
    assert second_result_full_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]
    second_result_partial_list = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_in_checkpoint_pk_defined_one_expectation_summary_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_summary,
    expectation_config_expect_column_values_to_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 1 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_summary,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_in_checkpoint_pk_defined_two_expectation_summary_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_summary,
    expectation_config_expect_column_values_to_be_in_set,
    expectation_config_expect_column_values_to_not_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - SUMMARY output, which means we have `partial_unexpected_index_list` only
        - 2 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_summary,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    # first and second expectations have same results. Although one is "expect_to_be"
    # and the other is "expect_to_not_be", they have opposite value_sets
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"][
        "partial_unexpected_index_list"
    ]
    assert first_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]

    second_result_full_list = evrs[0]["results"][1]["result"].get(
        "unexpected_index_list"
    )
    assert not second_result_full_list
    second_result_partial_list = evrs[0]["results"][1]["result"][
        "partial_unexpected_index_list"
    ]
    assert second_result_partial_list == [{"pk_1": 3}, {"pk_1": 4}, {"pk_1": 5}]


def test_result_format_in_checkpoint_pk_defined_one_expectation_basic_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_basic,
    expectation_config_expect_column_values_to_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - BASIC output, which means we have no unexpected_index_list output
        - 1 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_basic,
        expectations_list=[expectation_config_expect_column_values_to_be_in_set],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list


def test_result_format_in_checkpoint_pk_defined_two_expectation_basic_output(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_basic,
    expectation_config_expect_column_values_to_be_in_set,
    expectation_config_expect_column_values_to_not_be_in_set,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint only.
        - BASIC output, which means we have no unexpected_index_list output
        - 2 Expectations added to suite
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_basic,
        expectations_list=[
            expectation_config_expect_column_values_to_be_in_set,
            expectation_config_expect_column_values_to_not_be_in_set,
        ],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    # first and second expectations have "opposite" results since one is
    # "expect_to_be" and the other is "expect_to_not_be"
    first_result_full_list = evrs[0]["results"][0]["result"].get(
        "unexpected_index_list"
    )
    assert not first_result_full_list
    first_result_partial_list = evrs[0]["results"][0]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not first_result_partial_list

    second_result_full_list = evrs[0]["results"][1]["result"].get(
        "unexpected_index_list"
    )
    assert not second_result_full_list
    second_result_partial_list = evrs[0]["results"][1]["result"].get(
        "partial_unexpected_index_list"
    )
    assert not second_result_partial_list


@pytest.mark.xfail
@pytest.mark.unit
def test_result_format_in_checkpoint_one_expectation_overrides(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_defined_complete,
    expectation_config_expect_column_values_to_not_be_in_set_two_columns_defined,
):
    """
    What does this test?
        - unexpected_index_column defined in Checkpoint AND one expectation.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`

        - Checkpoint defines 1 column
        - Expectation defines 2 column

        - output contains 2 columns  (Expectation overrides Checkpoint config)

    When can xfail be removed?
        - When the logic for Expectation config overriding Checkpoint config is complete, xfail can be removed
    """
    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_defined_complete,
        expectations_list=[
            expectation_config_expect_column_values_to_not_be_in_set_two_columns_defined
        ],
    )
    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    first_result = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result == [
        {"pk_1": 3, "pk_2": "three"},
        {"pk_1": 4, "pk_2": "four"},
        {"pk_1": 5, "pk_2": "five"},
    ]


@pytest.mark.xfail
@pytest.mark.unit
def test_result_format_in_checkpoint_two_columns_two_expectation(
    data_context_with_connection_to_animal_names_db,
    checkpoint_dict_unexpected_index_column_names_not_defined,
    expectation_config_expect_column_values_to_not_be_in_set_two_columns_defined,
    expectation_config_expect_column_values_to_be_in_set_one_column_defined,
):
    """
    What does this test?
        - unexpected_index_column defined in 2 Expectations only.
        - COMPLETE output, which means we have `unexpected_index_list` and `partial_unexpected_index_list`

        - 1 Expectation defines 1 column
        - 1 Expectation defines 2 columns

        - output contains 1 column for 1 Expectation, and 2 columns for 1 Expectation.
            (Individual Expectation-level configurations are respected)

    When can xfail be removed?
        - When the logic for Expectation config overriding Checkpoint config is complete, xfail can be removed
    """

    context: DataContext = _add_expectations_and_checkpoint(
        data_context=data_context_with_connection_to_animal_names_db,
        checkpoint_config=checkpoint_dict_unexpected_index_column_names_not_defined,
        expectations_list=[
            expectation_config_expect_column_values_to_not_be_in_set_two_columns_defined,
            expectation_config_expect_column_values_to_be_in_set_one_column_defined,
        ],
    )

    result: CheckpointResult = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
    )
    evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

    first_result = evrs[0]["results"][0]["result"]["unexpected_index_list"]
    assert first_result == [
        {"pk_1": 3, "pk_2": "three"},
        {"pk_1": 4, "pk_2": "four"},
        {"pk_1": 5, "pk_2": "five"},
    ]

    second_result = evrs[0]["results"][1]["result"]["unexpected_index_list"]
    assert second_result == [
        {"pk_2": "three"},
        {"pk_2": "four"},
        {"pk_2": "five"},
    ]
