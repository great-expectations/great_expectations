from __future__ import annotations

import os
import random
from typing import TYPE_CHECKING, Callable, List, Tuple
from unittest import mock

import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
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
    return BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores
    )


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


@pytest.mark.xfail(
    reason="GX Cloud E2E tests are currently failing due to an id issue with ExpectationSuites; xfailing for purposes of the 0.15.20 release",
    run=True,
    strict=True,
)
@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
def test_get_validator_with_cloud_enabled_context_saves_expectation_suite_to_cloud_backend(
    mock_save_project_config: mock.MagicMock,
    prepare_validator_for_cloud_e2e: Callable[
        [CloudDataContext], Tuple[Validator, str]
    ],
) -> None:
    """
    What does this test do and why?

    Ensures that the Validator that is created through DataContext.get_validator() is
    Cloud-enabled if the DataContext used to instantiate the object is Cloud-enabled.
    Saving of ExpectationSuites using such a Validator should send payloads to the Cloud
    backend.
    """
    context = DataContext(cloud_mode=True)

    (
        validator,
        _,
    ) = prepare_validator_for_cloud_e2e(context)

    with mock.patch("requests.put", autospec=True) as mock_put:
        type(mock_put.return_value).status_code = mock.PropertyMock(return_value=200)
        validator.save_expectation_suite()

    assert mock_put.call_count == 1


@pytest.mark.xfail(
    reason="GX Cloud E2E tests are currently failing due to an id issue with ExpectationSuites; xfailing for purposes of the 0.15.20 release",
    run=True,
    strict=True,
)
@pytest.mark.e2e
@pytest.mark.cloud
@mock.patch("great_expectations.data_context.DataContext._save_project_config")
def test_validator_e2e_workflow_with_cloud_enabled_context(
    mock_save_project_config: mock.MagicMock,
    prepare_validator_for_cloud_e2e: Callable[
        [CloudDataContext], Tuple[Validator, str]
    ],
) -> None:
    """
    What does this test do and why?

    Ensures that the Validator that is created through DataContext.get_validator() is
    Cloud-enabled if the DataContext used to instantiate the object is Cloud-enabled.
    Saving of ExpectationSuites using such a Validator should send payloads to the Cloud
    backend.
    """
    context = DataContext(cloud_mode=True)

    (
        validator,
        expectation_suite_ge_cloud_id,
    ) = prepare_validator_for_cloud_e2e(context)

    assert len(validator.expectation_suite.expectations) == 4

    res = validator.expect_column_max_to_be_between("x", min_value=0, max_value=10)
    assert res.success

    assert len(validator.expectation_suite.expectations) == 5

    # Confirm that the suite is present before saving
    assert expectation_suite_ge_cloud_id in context.list_expectation_suite_names()
    suite_on_context = context.get_expectation_suite(
        ge_cloud_id=expectation_suite_ge_cloud_id
    )
    assert expectation_suite_ge_cloud_id == suite_on_context.ge_cloud_id

    validator.save_expectation_suite()
    assert len(validator.expectation_suite.expectations) == 5

    # Confirm that the suite is present after saving
    assert expectation_suite_ge_cloud_id in context.list_expectation_suite_names()
    suite_on_context = context.get_expectation_suite(
        ge_cloud_id=expectation_suite_ge_cloud_id
    )
    assert expectation_suite_ge_cloud_id == suite_on_context.ge_cloud_id
    assert len(suite_on_context.expectations) == 5
