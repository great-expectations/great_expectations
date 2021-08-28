import os

import pytest

import great_expectations
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.data_context.util import file_relative_path
from great_expectations.validator.validator import Validator


@pytest.fixture
def taxi_test_file():
    return file_relative_path(
        __file__,
        os.path.join(
            "..",
            "test_sets",
            "taxi_yellow_trip_data_samples",
            "yellow_trip_data_sample_2019-01.csv",
        ),
    )


@pytest.fixture
def taxi_test_file_directory():
    return file_relative_path(
        __file__,
        os.path.join(
            "..", "test_sets", "taxi_yellow_trip_data_samples", "first_3_files/"
        ),
    )


def test_get_batch_successful_specification_spark(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context = data_context_with_datasource_spark_engine
    batch_list: list = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
            runtime_parameters={
                "path": taxi_test_file_directory,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
            batch_spec_passthrough={
                "reader_method": "csv",
                "reader_options": {"header": True},
            },
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch = batch_list[0]
    assert batch.data.dataframe.count() == 30000


def test_get_batch_successful_specification_spark_batch_spec_in_config(
    data_context_with_datasource_spark_engine_batch_spec_passthrough,
    taxi_test_file_directory,
    spark_session,
):
    context = data_context_with_datasource_spark_engine_batch_spec_passthrough
    batch_list: list = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",  # this can be anything that identifies this data
            runtime_parameters={
                "path": taxi_test_file_directory,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
            # batch_spec_passthrough={
            #                       "reader_method": "csv",
            #                       "reader_options": {"header": True},
            # },
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch = batch_list[0]
    assert batch.data.dataframe.count() == 30000


def test_get_batch_successful_specification_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    batch_list: list = context.get_batch_list(
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


def test_get_batch_ambiguous_parameter_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    """
    What does this test and why?

    get_batch_list() requires batch_request to be passed in a named parameter. This test passes in a batch_request
    as an unnamed parameter, which will raise a GreatExpectationsTypeError
    """
    context = data_context_with_datasource_sqlalchemy_engine
    # raised by get_batch_list()
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_batch_failed_specification_type_error_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
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


def test_get_batch_failed_specification_no_batch_identifier_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # batch_identifiers missing (set to None)
        batch: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers=None,
            )
        )

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # batch_identifiers missing (omitted)
        batch: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
            )
        )


def test_get_batch_failed_specification_no_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # runtime_parameters missing (None)
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters=None,
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # runtime_parameters missing (omitted)
        batch: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_batch_failed_specification_incorrect_batch_spec_passthrough_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # incorrect batch_spec_passthrough, which should be a dict
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


def test_get_batch_failed_specification_wrong_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    # raised by _validate_runtime_parameters() in RuntimeDataConnector
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # runtime_parameters are not configured in the DataConnector
        batch: list = context.get_batch_list(
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


def test_get_validator_ambiguous_parameter_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    """
    What does this test and why?

    get_batch_list() requires batch_request to be passed in a named parameter. This test passes in a batch_request
    as an unnamed parameter, which will raise a GreatExpectationsTypeError
    """
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    # raised by get_batch_list() in DataContext
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: list = context.get_validator(
            RuntimeBatchRequest(
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


def test_get_validator_wrong_type_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    # data_connector_name should be a dict not an int
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


def test_get_validator_failed_specification_no_batch_identifier_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    # batch_identifiers should not be None
    with pytest.raises(TypeError):
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers=None,
            ),
            expectation_suite_name="my_expectations",
        )

    # batch_identifiers should not be omitted
    with pytest.raises(TypeError):
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_failed_specification_incorrect_batch_spec_passthrough_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # incorrect batch_spec_passthrough, which should be a dict
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "query": "SELECT * from table_partitioned_by_date_column__A LIMIT 10"
                },
                batch_identifiers={"default_identifier_name": "identifier_name"},
                batch_spec_passthrough=1,
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_failed_specification_no_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    with pytest.raises(TypeError):
        # runtime_parameters should not be None
        batch: list = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters=None,
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # runtime_parameters missing (omitted)
        batch: list = context.get_validator(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_get_validator_wrong_runtime_parameters_sqlalchemy_engine(
    data_context_with_datasource_sqlalchemy_engine, sa
):
    context = data_context_with_datasource_sqlalchemy_engine
    context.create_expectation_suite("my_expectations")
    # raised by _validate_runtime_parameters() in RuntimeDataConnector
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidBatchRequestError
    ):
        # runtime_parameters are not configured in the DataConnector
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
