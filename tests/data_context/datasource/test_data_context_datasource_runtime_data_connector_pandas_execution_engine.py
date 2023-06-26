from typing import TYPE_CHECKING, Dict, List

import pytest

import great_expectations
import great_expectations.exceptions as gx_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.core.id_dict import BatchSpec
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    import pandas as pd

yaml = YAMLHandler()

#########################################
# Tests with data passed in as batch_data
#########################################


# Tests with PandasExecutionEngine : batch_data
def test_batch_data_get_batch_successful_specification_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas

    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",
            runtime_parameters={"batch_data": test_df},
            batch_identifiers={"default_identifier_name": "identifier_name"},
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)


def test_batch_data_get_batch_successful_specification_pandas_engine_named_asset(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"batch_data": test_df},
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)

    batch_1: Batch = batch_list[0]
    assert batch_1.batch_definition.batch_identifiers == batch_identifiers


def test_batch_data_get_batch_successful_specification_pandas_engine_named_asset_two_batch_requests(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas

    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"batch_data": test_df},
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
            runtime_parameters={"batch_data": test_df},
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch_2: Batch = batch_list[0]
    assert batch_2.batch_definition.batch_identifiers == batch_identifiers


def test_get_batch_failed_specification_wrong_runtime_parameters_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine

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


def test_batch_data_get_validator_successful_specification_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas

    context.add_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator: Validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="default_data_asset_name",
            runtime_parameters={"batch_data": test_df},
            batch_identifiers={"default_identifier_name": "identifier_name"},
        ),
        expectation_suite_name="my_expectations",
    )
    assert isinstance(my_validator, Validator)


def test_batch_data_get_validator_successful_specification_pandas_engine_named_asset(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas

    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    context.add_expectation_suite("my_expectations")

    # Successful specification using a RuntimeBatchRequest
    my_validator: Validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"batch_data": test_df},
            batch_identifiers=batch_identifiers,
        ),
        expectation_suite_name="my_expectations",
    )
    assert isinstance(my_validator, Validator)
    assert (
        my_validator.active_batch.batch_definition.batch_identifiers
        == batch_identifiers
    )


def test_batch_data_get_validator_ambiguous_parameter_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    """
    What does this test and why?

    get_batch_list() requires batch_request to be passed in a named parameter. This test passes in a batch_request
    as an unnamed parameter, which will raise a GreatExpectationsTypeError
    """
    context: DataContext = data_context_with_datasource_pandas_engine
    test_df: pd.DataFrame = test_df_pandas

    context.add_expectation_suite("my_expectations")
    # raised by get_batch_list() in DataContext
    with pytest.raises(TypeError):
        context.get_validator(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_wrong_type_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine

    context.add_expectation_suite("my_expectations")

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


def test_batch_data_get_validator_wrong_runtime_parameters_pandas_engine(
    data_context_with_datasource_pandas_engine, test_df_pandas
):
    context: DataContext = data_context_with_datasource_pandas_engine

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


###################################
# Tests with data passed in as path
###################################


# Tests with Pandas Execution Engine
def test_file_path_get_batch_successful_specification_pandas(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={
                "path": taxi_test_file,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
        )
    )
    assert len(batch_list) == 1
    my_batch_1 = batch_list[0]
    assert isinstance(my_batch_1.batch_spec, BatchSpec)
    assert my_batch_1.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(my_batch_1, Batch)
    assert isinstance(my_batch_1.data, PandasBatchData)
    assert len(my_batch_1.data.dataframe) == 10000
    assert len(my_batch_1.data.dataframe.columns) == 18


def test_file_path_get_batch_successful_specification_pandas_file_path_no_headers(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={
                "path": taxi_test_file,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
            batch_spec_passthrough={"reader_options": {"header": None}},
        )
    )
    assert len(batch_list) == 1
    my_batch_1 = batch_list[0]
    assert isinstance(my_batch_1.batch_spec, BatchSpec)
    assert my_batch_1.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(my_batch_1, Batch)
    assert isinstance(my_batch_1.data, PandasBatchData)
    assert (
        len(my_batch_1.data.dataframe) == 10001
    )  # one more line because of header being set to None
    assert len(my_batch_1.data.dataframe.columns) == 18


def test_file_path_get_batch_pandas_not_supported_directory(
    data_context_with_datasource_pandas_engine, taxi_test_file_directory
):
    context: DataContext = data_context_with_datasource_pandas_engine
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "path": taxi_test_file_directory,
                },
                batch_identifiers={"default_identifier_name": 1234567890},
            )
        )


def test_get_batch_pandas_wrong_path(data_context_with_datasource_pandas_engine):
    context: DataContext = data_context_with_datasource_pandas_engine
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "path": "i_dont_exist",
                },
                batch_identifiers={"default_identifier_name": 1234567890},
            )
        )


def test_file_path_get_batch_pandas_wrong_reader_method(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    with pytest.raises(gx_exceptions.ExecutionEngineError):
        context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="my_data_asset",
                runtime_parameters={
                    "path": taxi_test_file,
                },
                batch_identifiers={"default_identifier_name": 1234567890},
                batch_spec_passthrough={"reader_method": "i_am_not_valid"},
            )
        )


def test_file_path_get_batch_successful_specification_pandas_engine_named_asset(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"path": taxi_test_file},
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)

    batch_1: Batch = batch_list[0]
    assert batch_1.batch_definition.batch_identifiers == batch_identifiers


def test_file_path_get_batch_successful_specification_pandas_engine_named_asset_two_batch_requests(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"path": taxi_test_file},
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
            runtime_parameters={"path": taxi_test_file},
            batch_identifiers=batch_identifiers,
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch_2: Batch = batch_list[0]
    assert batch_2.batch_definition.batch_identifiers == batch_identifiers


def test_file_path_get_validator_successful_specification_pandas_engine_named_asset(
    data_context_with_datasource_pandas_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_pandas_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    context.add_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator: Validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="asset_a",
            runtime_parameters={"path": taxi_test_file},
            batch_identifiers=batch_identifiers,
        ),
        expectation_suite_name="my_expectations",
    )
    assert isinstance(my_validator, Validator)
    assert (
        my_validator.active_batch.batch_definition.batch_identifiers
        == batch_identifiers
    )
