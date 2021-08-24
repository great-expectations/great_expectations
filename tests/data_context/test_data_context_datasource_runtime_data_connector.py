import pytest

import great_expectations
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.validator.validator import Validator


def test_get_batch_successful_specification(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    batch_list: list = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
            runtime_parameters={
                "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
            },
            batch_identifiers={"default_identifier_name": "identifier_name"},
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)


def test_get_batch_ambiguous_parameter(
    data_context_with_datasource_sqlalchemy_engine,
):
    """
    What does this test and why?

    get_batch_list requires that RuntimeBatchRequest be passed in as a named parameter: 'batch_request'. This test
    passes in a valid RuntimeBatchRequest, but expects GE to raise a GreatExpectationsTypeError.
    """
    context = data_context_with_datasource_sqlalchemy_engine
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_batch_failed_specification_type_error(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    with pytest.raises(TypeError):
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=1,  # wrong data_type
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


# this test should be working
def test_get_batch_failed_specification_no_batch_identifier(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine

    with pytest.raises(TypeError):
        # batch_identifiers missing
        batch: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers=None,
            )
        )


def test_get_batch_failed_specification_no_runtime_parameters(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine

    with pytest.raises(TypeError):
        # batch_identifiers missing
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters=None,
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_batch_failed_specification_incorrect_batch_spec_passthrough(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    with pytest.raises(TypeError):
        # batch_identifiers missing
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
                batch_spec_passthrough=1,
            )
        )


def test_get_batch_failed_specification_wrong_runtime_parameters(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # batch_identifiers missing
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"i_dont_exist": "i_dont_either"},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_validator_successful_specification(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator = context.get_validator(
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


def test_get_validator_ambiguous_parameter(
    data_context_with_datasource_sqlalchemy_engine,
):
    """
    What does this test and why?

    get_batch_list, which is called by get_validator() requires that RuntimeBatchRequest be passed in as a named parameter: 'batch_request'. This test
    passes in a valid RuntimeBatchRequest, but expects GE to raise a GreatExpectationsTypeError.
    """
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: list = context.get_validator(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_wrong_type(data_context_with_datasource_sqlalchemy_engine):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")

    # Failed specification using an untyped BatchRequest
    with pytest.raises(TypeError):
        context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name=1,
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_failed_specification_no_batch_identifier(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")

    with pytest.raises(TypeError):
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers=None,
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_failed_specification_incorrect_batch_spec_passthrough(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")

    with pytest.raises(TypeError):
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
                batch_spec_passthrough=1,  # needs to be a dict
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_failed_specification_no_runtime_parameters(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    with pytest.raises(TypeError):
        # batch_identifiers_missing
        batch: list = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
                runtime_parameters=None,
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_wrong_runtime_parameters(
    data_context_with_datasource_sqlalchemy_engine,
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # batch_identifiers missing
        batch: list = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"i_dont_exist": "i_dont_either"},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )
