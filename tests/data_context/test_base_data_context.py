from __future__ import annotations

import os
import random
from typing import TYPE_CHECKING, Callable, List, Tuple

import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.validator.validator import Validator

from great_expectations.data_context.store import (  # isort:skip
    ExpectationsStore,
    ValidationsStore,
    EvaluationParameterStore,
)

if TYPE_CHECKING:
    from great_expectations.data_context import (
        AbstractDataContext,
        EphemeralDataContext,
    )

yaml = YAMLHandler()


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
) -> AbstractDataContext:
    with pytest.deprecated_call():
        context = BaseDataContext(
            project_config=basic_in_memory_data_context_config_just_stores
        )
    return context


@pytest.mark.unit
def test_instantiation_and_basic_stores(
    basic_in_memory_data_context_just_stores,
    basic_in_memory_data_context_config_just_stores,
):
    context: EphemeralDataContext = basic_in_memory_data_context_just_stores
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


@pytest.mark.unit
def test_config_variables(basic_in_memory_data_context_just_stores):
    # nothing instantiated yet
    assert basic_in_memory_data_context_just_stores.config_variables == {}


@pytest.mark.unit
def test_list_stores(basic_in_memory_data_context_just_stores):
    assert basic_in_memory_data_context_just_stores.list_stores() == [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
    ]


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.unit
def test_get_config_with_variables_substituted(
    basic_in_memory_data_context_just_stores,
):
    """
    Basic test for get_config_with_variables_substituted()
    A more thorough set of tests exist in test_data_context_config_variables.py with
    test_setting_config_variables_is_visible_immediately() testing whether the os.env values take
    precedence over config_file values, which they should.
    """

    context: EphemeralDataContext = basic_in_memory_data_context_just_stores
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


@pytest.fixture
def prepare_validator_for_cloud_e2e() -> (
    Callable[[CloudDataContext], Tuple[Validator, str]]
):
    def _closure(context: CloudDataContext) -> Tuple[Validator, str]:
        # Create a suite to be used in Validator instantiation
        suites = context.list_expectation_suites()
        expectation_suite_ge_cloud_id = suites[0].id

        # To ensure we don't accidentally impact parallel test runs in Azure, we randomly generate a suite name in this E2E test.
        # To limit the number of generated suites, we limit the randomization to 20 numbers.
        rand_suffix = random.randint(1, 20)
        suite_name = f"oss_e2e_test_suite_{rand_suffix}"

        # Start off each test run with a clean slate
        if expectation_suite_ge_cloud_id in context.list_expectation_suite_names():
            context.delete_expectation_suite(ge_cloud_id=expectation_suite_ge_cloud_id)

        suite = context.add_expectation_suite(
            suite_name,
            ge_cloud_id=expectation_suite_ge_cloud_id,
        )

        # Set up a number of Expectations and confirm proper assignment
        configs = [  # Content of configs do not matter as they are simply used to populate the suite (never run)
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "infinities"},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist", kwargs={"column": "nulls"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist", kwargs={"column": "naturals"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "naturals"},
            ),
        ]
        for config in configs:
            suite.add_expectation(expectation_configuration=config)

        suite.ge_cloud_id = expectation_suite_ge_cloud_id
        context.add_or_update_expectation_suite(
            expectation_suite=suite,
        )

        assert len(suite.expectations) == 4

        # Grab the first datasource/data connector/data asset bundle we can to use in Validation instantiation
        datasource_name = "Test Pandas Datasource"
        datasource = context.datasources[datasource_name]

        data_connector = tuple(datasource.data_connectors.values())[0]
        data_connector_name = data_connector.name

        data_asset_name = tuple(data_connector.assets.keys())[0]

        batch_request = RuntimeBatchRequest(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            runtime_parameters={
                "batch_data": pd.DataFrame({"x": range(10)}),
            },
            batch_identifiers={"col1": "123"},
        )

        # Create Validator and ensure that persistence is Cloud-backed
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
        )

        # Ensure that the Expectations set above propogate down successfully
        assert len(validator.expectation_suite.expectations) == 4

        return validator, expectation_suite_ge_cloud_id

    return _closure
