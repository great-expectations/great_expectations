from typing import Dict, List

import pytest

import great_expectations
from great_expectations import DataContext
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()

####################################
# Tests with data passed in as query
####################################


def test_get_batch_successful_specification_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context: DataContext = data_context_with_datasource_sqlalchemy_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers={"default_identifier_name": "identifier_name"},
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)


def test_get_batch_successful_specification_sqlalchemy_engine_named_asset(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context: DataContext = data_context_with_datasource_sqlalchemy_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)

    batch_1: Batch = batch_list[0]
    assert batch_1.batch_definition.batch_identifiers == batch_identifiers


def test_get_batch_successful_specification_pandas_engine_named_asset_two_batch_requests(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context: DataContext = data_context_with_datasource_sqlalchemy_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch_1: Batch = batch_list[0]
    assert batch_1.batch_definition.batch_identifiers == batch_identifiers

    batch_identifiers: Dict[str, int] = {"day": 2, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch_2: Batch = batch_list[0]
    assert batch_2.batch_definition.batch_identifiers == batch_identifiers


def test_get_batch_failed_specification_wrong_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    # raised by _validate_runtime_parameters() in RuntimeDataConnector
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # runtime_parameters are not configured in the DataConnector
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"i_dont_exist": "i_dont_either"},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_validator_successful_specification_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.add_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator: Validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers={"default_identifier_name": "identifier_name"},
        ),
        expectation_suite_name="my_expectations",
    )
    assert isinstance(my_validator, Validator)


def test_get_validator_wrong_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context: DataContext = data_context_with_datasource_sqlalchemy_engine
    context.add_expectation_suite("my_expectations")
    # raised by _validate_runtime_parameters() in RuntimeDataConnector
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # runtime_parameters are not configured in the DataConnector
        context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"i_dont_exist": "i_dont_either"},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_successful_specification_sqlalchemy_engine_named_asset(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context: DataContext = data_context_with_datasource_sqlalchemy_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    context.add_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator: Validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers=batch_identifiers,
        ),
        expectation_suite_name="my_expectations",
    )
    assert isinstance(my_validator, Validator)
    assert (
        my_validator.active_batch.batch_definition.batch_identifiers
        == batch_identifiers
    )
