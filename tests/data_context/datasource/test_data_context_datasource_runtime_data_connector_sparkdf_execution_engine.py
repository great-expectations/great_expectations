from typing import Dict, List

import pandas as pd
import pytest

import great_expectations
import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.core.id_dict import BatchSpec
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.validator.validator import Validator

yaml = YAMLHandler()


@pytest.fixture()
def test_df_spark(spark_session):
    test_df: "pyspark.sql.dataframe.DataFrame" = spark_session.createDataFrame(
        data=pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    )
    return test_df


@pytest.fixture
def data_context_with_datasource_spark_engine_batch_spec_passthrough(
    empty_data_context, spark_session
):
    context: DataContext = empty_data_context
    config = yaml.load(
        """
    class_name: Datasource
    execution_engine:
        class_name: SparkDFExecutionEngine
    data_connectors:
        default_runtime_data_connector_name:
            class_name: RuntimeDataConnector
            batch_identifiers:
                - default_identifier_name
            batch_spec_passthrough:
                reader_method: csv
                reader_options:
                    header: True
        """,
    )
    context.add_datasource(
        "my_datasource",
        **config,
    )
    return context


#########################################
# Tests with data passed in as batch_data
#########################################

# Tests with SparkDFExecutionEngine : batch_data
def test_batch_data_get_batch_successful_specification_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_batch_successful_specification_sparkdf_engine_named_asset(
    data_context_with_datasource_spark_engine, test_df_spark
):
    context: "DataContext" = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = (
        test_df_spark  # noqa: F821  # noqa: F821
    )
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


def test_batch_data_get_batch_successful_specification_sparkdf_engine_named_asset_two_batch_requests(
    data_context_with_datasource_spark_engine, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_batch_ambiguous_parameter_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    """
    What does this test and why?

    get_batch_list() requires batch_request to be passed in a named parameter. This test passes in a batch_request
    as an unnamed parameter, which will raise a GreatExpectationsTypeError
    """
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    # raised by get_batch_list()
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: List[Batch] = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_batch_data_get_batch_failed_specification_type_error_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=1,  # wrong data_type
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            )
        )


def test_batch_data_get_batch_failed_specification_no_batch_identifier_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # batch_identifiers missing (set to None)
        batch: list = context.get_batch_list(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
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
                runtime_parameters={"batch_data": test_df},
            )
        )


def test_batch_data_get_batch_failed_specification_no_runtime_parameters_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_batch_failed_specification_incorrect_batch_spec_passthrough_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # incorrect batch_spec_passthrough, which should be a dict
        batch: list = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
                batch_spec_passthrough=1,
            )
        )


def test_get_batch_failed_specification_wrong_runtime_parameters_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_validator_successful_specification_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    context.create_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator = context.get_validator(
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


def test_batch_data_get_validator_ambiguous_parameter_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    """
    What does this test and why?

    get_batch_list() requires batch_request to be passed in a named parameter. This test passes in a batch_request
    as an unnamed parameter, which will raise a GreatExpectationsTypeError
    """
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    context.create_expectation_suite("my_expectations")
    # raised by get_batch_list() in DataContext
    with pytest.raises(ge_exceptions.GreatExpectationsTypeError):
        batch_list: List[Batch] = context.get_validator(
            RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
            ),
            expectation_suite_name="my_expectations",
        )


def test_get_validator_wrong_type_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_validator_failed_specification_no_batch_identifier_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    context.create_expectation_suite("my_expectations")

    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    # batch_identifiers should not be None
    with pytest.raises(TypeError):
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
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
                runtime_parameters={"batch_data": test_df},
            ),
            expectation_suite_name="my_expectations",
        )


def test_batch_data_get_validator_failed_specification_incorrect_batch_spec_passthrough_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

    context.create_expectation_suite("my_expectations")
    # raised by _validate_runtime_batch_request_specific_init_parameters() in RuntimeBatchRequest.__init__()
    with pytest.raises(TypeError):
        # incorrect batch_spec_passthrough, which should be a dict
        validator: Validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={"batch_data": test_df},
                batch_identifiers={"default_identifier_name": "identifier_name"},
                batch_spec_passthrough=1,
            ),
            expectation_suite_name="my_expectations",
        )


def test_batch_data_get_validator_failed_specification_no_runtime_parameters_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: DataContext = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821

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


def test_batch_data_get_validator_wrong_runtime_parameters_sparkdf_engine(
    data_context_with_datasource_spark_engine, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine
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


def test_batch_data_get_validator_successful_specification_sparkdf_engine_named_asset(
    data_context_with_datasource_spark_engine, spark_session, test_df_spark
):
    context: "DataContext" = data_context_with_datasource_spark_engine
    test_df: "pyspark.sql.dataframe.DataFrame" = test_df_spark  # noqa: F821
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    context.create_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator = context.get_validator(
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


###################################
# Tests with data passed in as path
###################################


# Tests with SparkDF Execution Engine


def test_file_path_sparkdf_execution_engine_batch_definition_list_from_batch_request_success(
    data_context_with_datasource_spark_engine,
    taxi_test_file,
    spark_session,
):
    context: DataContext = data_context_with_datasource_spark_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={
                "path": taxi_test_file,
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
    assert isinstance(batch.batch_spec, BatchSpec)
    assert batch.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(batch, Batch)
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.count() == 10000  # 3 files read in as 1
    assert len(batch.data.dataframe.columns) == 18


def test_file_path_get_batch_spark_directory_fail_no_reader_method(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        batch_list: List[Batch] = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "path": taxi_test_file_directory,
                },
                batch_identifiers={"default_identifier_name": 1234567890},
            )
        )


def test_file_path_get_batch_spark_directory_fail_wrong_reader_method(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        batch_list: List[Batch] = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "path": taxi_test_file_directory,
                },
                batch_identifiers={"default_identifier_name": 1234567890},
                batch_spec_passthrough={
                    "reader_options": {"header": True},
                    "reader_method": "i_am_invalid",
                },
            )
        )


def test_file_path_sparkdf_execution_engine_batch_definition_list_from_batch_request_success_file_path_no_header(
    data_context_with_datasource_spark_engine,
    taxi_test_file,
    spark_session,
):
    context: DataContext = data_context_with_datasource_spark_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={
                "path": taxi_test_file,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
            batch_spec_passthrough={
                "reader_method": "csv",
            },
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch = batch_list[0]
    assert isinstance(batch.batch_spec, BatchSpec)
    assert batch.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(batch, Batch)
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.count() == 10001  # no header by default
    assert len(batch.data.dataframe.columns) == 18


def test_file_path_get_batch_successful_specification_spark_directory(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
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
    assert isinstance(batch.batch_spec, BatchSpec)
    assert batch.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(batch, Batch)
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.count() == 30000  # 3 files read in as 1
    assert len(batch.data.dataframe.columns) == 18


def test_file_path_get_batch_successful_specification_spark_directory_batch_spec_passthrough_in_config(
    data_context_with_datasource_spark_engine_batch_spec_passthrough,
    taxi_test_file_directory,
    spark_session,
):
    """
    What does this test and why?

    This tests the same behavior as the previous test, test_get_batch_successful_specification_spark_directory, but the
    batch_spec_passthrough is in the Datasource configuration, found in the data_context_with_datasource_spark_engine_batch_spec_passthrough
    fixture. This is why the `batch_spec_passthrough` parameters are commented out, but GX is still able to read in the 3 CSV files
    as a single SparkDF with 30,000 lines.

    """
    context: DataContext = (
        data_context_with_datasource_spark_engine_batch_spec_passthrough
    )
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
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
    assert isinstance(batch.batch_spec, BatchSpec)
    assert batch.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(batch, Batch)
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.count() == 30000  # 3 files read in as 1
    assert len(batch.data.dataframe.columns) == 18


def test_file_path_get_batch_successful_specification_spark_directory_no_header(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine
    batch_list: List[Batch] = context.get_batch_list(
        batch_request=RuntimeBatchRequest(
            datasource_name="my_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="my_data_asset",
            runtime_parameters={
                "path": taxi_test_file_directory,
            },
            batch_identifiers={"default_identifier_name": 1234567890},
            batch_spec_passthrough={
                "reader_method": "csv",
            },
        )
    )
    assert len(batch_list) == 1
    assert isinstance(batch_list[0], Batch)
    batch = batch_list[0]
    assert isinstance(batch.batch_spec, BatchSpec)
    assert batch.batch_definition["data_asset_name"] == "my_data_asset"
    assert isinstance(batch, Batch)
    assert isinstance(batch.data, SparkDFBatchData)
    assert batch.data.dataframe.count() == 30003  # 3 files read in as 1
    assert len(batch.data.dataframe.columns) == 18


def test_file_path_get_batch_spark_fail_wrong_file_path(
    data_context_with_datasource_spark_engine, taxi_test_file_directory, spark_session
):
    context: DataContext = data_context_with_datasource_spark_engine

    # raised by get_batch_data_and_markers() in SparkDFExecutionEngine.
    with pytest.raises(ge_exceptions.ExecutionEngineError):
        batch_list: List[Batch] = context.get_batch_list(
            batch_request=RuntimeBatchRequest(
                datasource_name="my_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="default_data_asset_name",
                runtime_parameters={
                    "path": "I_dont_exist",
                },
                batch_identifiers={"default_identifier_name": 1234567890},
                batch_spec_passthrough={
                    "reader_method": "csv",
                },
            )
        )


def test_file_path_get_batch_successful_specification_sparkdf_engine_named_asset(
    data_context_with_datasource_spark_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_spark_engine
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


def test_file_path_get_batch_successful_specification_sparkdf_engine_named_asset_two_batch_requests(
    data_context_with_datasource_spark_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_spark_engine
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


def test_file_path_get_validator_successful_specification_sparkdf_engine_named_asset(
    data_context_with_datasource_spark_engine, taxi_test_file
):
    context: DataContext = data_context_with_datasource_spark_engine
    batch_identifiers: Dict[str, int] = {"day": 1, "month": 12}
    context.create_expectation_suite("my_expectations")
    # Successful specification using a RuntimeBatchRequest
    my_validator = context.get_validator(
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
