from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context import DataContext


def test_expect_table_row_count_to_be_between_runtime_custom_query_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request = RuntimeBatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="titanic",
        runtime_parameters={"query": "select * from titanic"},
        batch_identifiers={"default_identifier_name": "test_identifier"},
        batch_spec_passthrough={"create_temp_table": False},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )
    results = validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=2000
    )
    assert results == ExpectationValidationResult(
        success=True,
        result={"observed_value": 1313},
        meta={},
        expectation_config={
            "kwargs": {
                "min_value": 100,
                "max_value": 2000,
                "batch_id": "a47a711a9984cb2a482157adf54c3cb6",
            },
            "ge_cloud_id": None,
            "meta": {},
            "expectation_type": "expect_table_row_count_to_be_between",
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )


def test_expect_table_row_count_to_be_between_runtime_custom_query_with_where_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request = RuntimeBatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="titanic",
        runtime_parameters={"query": "select * from titanic where sexcode = 1"},
        batch_identifiers={"default_identifier_name": "test_identifier"},
        batch_spec_passthrough={"create_temp_table": False},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )
    results = validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=2000
    )
    assert results == ExpectationValidationResult(
        success=True,
        result={"observed_value": 462},
        meta={},
        expectation_config={
            "kwargs": {
                "min_value": 100,
                "max_value": 2000,
                "batch_id": "a47a711a9984cb2a482157adf54c3cb6",
            },
            "ge_cloud_id": None,
            "meta": {},
            "expectation_type": "expect_table_row_count_to_be_between",
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )


def test_expect_table_row_count_to_be_between_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request = BatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="titanic",
        batch_spec_passthrough={"create_temp_table": False},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )
    results = validator.expect_table_row_count_to_be_between(
        min_value=100, max_value=2000
    )
    assert results == ExpectationValidationResult(
        success=True,
        result={"observed_value": 1313},
        meta={},
        expectation_config={
            "kwargs": {
                "min_value": 100,
                "max_value": 2000,
                "batch_id": "a47a711a9984cb2a482157adf54c3cb6",
            },
            "ge_cloud_id": None,
            "meta": {},
            "expectation_type": "expect_table_row_count_to_be_between",
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )
