import os
from typing import List
from unittest import mock

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig

from great_expectations.data_context.store import (  # isort:skip
    ExpectationsStore,
    ValidationsStore,
    EvaluationParameterStore,
)

yaml: YAMLHandler = YAMLHandler()


@pytest.fixture()
def basic_in_memory_data_context_config_just_stores():
    return DataContextConfig(
        config_version=3.0,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )


@pytest.fixture()
def basic_in_memory_data_context_just_stores(
    basic_in_memory_data_context_config_just_stores,
):
    return BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores
    )


def test_instantiation_and_basic_stores(
    basic_in_memory_data_context_just_stores,
    basic_in_memory_data_context_config_just_stores,
):
    context: BaseDataContext = basic_in_memory_data_context_just_stores
    # TODO <WILL> - config is basic_in_memory_data_context_config_just_stores + global overrides. Add test for this
    assert len(context.stores) == 3

    assert context.expectations_store_name == "expectations_store"
    assert isinstance(context.expectations_store, ExpectationsStore)

    assert context.validations_store_name == "validation_result_store"
    assert isinstance(context.validations_store, ValidationsStore)

    assert context.evaluation_parameter_store_name == "evaluation_parameter_store"
    assert isinstance(context.evaluation_parameter_store, EvaluationParameterStore)

    # TODO : validation_store
    # TODO : checkpoint store
    # TODO : metric store?


def test_config_variables(basic_in_memory_data_context_just_stores):
    # nothing instantiated yet
    assert basic_in_memory_data_context_just_stores.config_variables == {}


def test_list_stores(basic_in_memory_data_context_just_stores):
    assert basic_in_memory_data_context_just_stores.list_stores() == [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
    ]


def test_add_store(basic_in_memory_data_context_just_stores):
    store_name: str = "my_new_expectations_store"
    store_config: dict = {"class_name": "ExpectationsStore"}
    basic_in_memory_data_context_just_stores.add_store(
        store_name=store_name, store_config=store_config
    )
    assert basic_in_memory_data_context_just_stores.list_stores() == [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
        {"class_name": "ExpectationsStore", "name": "my_new_expectations_store"},
    ]


def test_list_active_stores(basic_in_memory_data_context_just_stores):
    """
    Active stores are identified by the following keys:
        expectations_store_name,
        validations_store_name,
        evaluation_parameter_store_name,
        checkpoint_store_name
        profiler_store_name
    Therefore the test also test that the list_active_stores() output doesn't change after a store named
    `my_new_expectations_store` is added
    """
    expected_store_list: List[dict] = [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
    ]
    assert (
        basic_in_memory_data_context_just_stores.list_active_stores()
        == expected_store_list
    )

    store_name: str = "my_new_expectations_store"
    store_config: dict = {"class_name": "ExpectationsStore"}
    basic_in_memory_data_context_just_stores.add_store(
        store_name=store_name, store_config=store_config
    )
    assert (
        basic_in_memory_data_context_just_stores.list_active_stores()
        == expected_store_list
    )


def test_get_config_with_variables_substituted(
    basic_in_memory_data_context_just_stores,
):
    """
    Basic test for get_config_with_variables_substituted()
    A more thorough set of tests exist in test_data_context_config_variables.py with
    test_setting_config_variables_is_visible_immediately() testing whether the os.env values take
    precedence over config_file values, which they should.
    """

    context: BaseDataContext = basic_in_memory_data_context_just_stores
    assert isinstance(context.get_config(), DataContextConfig)

    # override the project config to use the $ escaped variable
    context._project_config["validations_store_name"] = "${replace_me}"
    try:
        # which we set from the environment
        os.environ["replace_me"] = "value_from_env_var"
        assert (
            context.get_config_with_variables_substituted().validations_store_name
            == "value_from_env_var"
        )
    finally:
        del os.environ["replace_me"]


@pytest.mark.cloud
@mock.patch("great_expectations.data_context.CloudDataContext.create_expectation_suite")
def test_create_expectation_suite_with_cloud_enabled_context_uses_cloud_impl(
    mock_cloud_create_expectation_suite: mock.MagicMock,
    empty_cloud_data_context: BaseDataContext,
) -> None:
    """
    What does this test do and why?

    Ensures that when a BaseDataContext is instantiated with cloud_mode=True, the call to
    ExpectationSuite CRUD leverages the implementation defined and managed by CloudDataContext.
    """
    context = empty_cloud_data_context

    context.create_expectation_suite("my_expectation_suite")
    assert mock_cloud_create_expectation_suite.call_count == 1


@pytest.mark.cloud
@mock.patch("great_expectations.data_context.CloudDataContext.get_expectation_suite")
def test_get_expectation_suite_with_cloud_enabled_context_uses_cloud_impl(
    mock_cloud_get_expectation_suite: mock.MagicMock,
    empty_cloud_data_context: BaseDataContext,
) -> None:
    """
    What does this test do and why?

    Ensures that when a BaseDataContext is instantiated with cloud_mode=True, the call to
    ExpectationSuite CRUD leverages the implementation defined and managed by CloudDataContext.
    """
    context = empty_cloud_data_context

    context.get_expectation_suite("my_expectation_suite")
    assert mock_cloud_get_expectation_suite.call_count == 1


@pytest.mark.cloud
@mock.patch("great_expectations.data_context.CloudDataContext.delete_expectation_suite")
def test_delete_expectation_suite_with_cloud_enabled_context_uses_cloud_impl(
    mock_cloud_delete_expectation_suite: mock.MagicMock,
    empty_cloud_data_context: BaseDataContext,
) -> None:
    """
    What does this test do and why?

    Ensures that when a BaseDataContext is instantiated with cloud_mode=True, the call to
    ExpectationSuite CRUD leverages the implementation defined and managed by CloudDataContext.
    """
    context = empty_cloud_data_context

    context.delete_expectation_suite("my_expectation_suite")
    assert mock_cloud_delete_expectation_suite.call_count == 1


@pytest.mark.cloud
def test_get_validator_with_cloud_enabled_context_passes_cloud_context_to_return_obj() -> None:
    context = DataContext(ge_cloud_mode=True)

    # expectation_suite_ge_cloud_id = "1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p"
    # context.create_expectation_suite("my_test_suite")

    # validator = context.get_validator(
    #     expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
    # )

    # assert isinstance(validator.data_context, CloudDataContext)
