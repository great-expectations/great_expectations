import pandas as pd
import pytest

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.expectations.expect_queried_table_row_count_to_be import (
    ExpectQueriedTableRowCountToBe,
)
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.self_check.util import build_spark_validator_with_data
from great_expectations.validator.validator import (
    ExpectationValidationResult,
    Validator,
)

sqlite_runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
    datasource_name="my_sqlite_db_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="titanic",
    runtime_parameters={"query": "SELECT * FROM titanic LIMIT 100"},
    batch_identifiers={"default_identifier_name": "test_identifier"},
    batch_spec_passthrough={"create_temp_table": False},
)

sqlite_batch_request: BatchRequest = BatchRequest(
    datasource_name="my_sqlite_db_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="titanic",
    batch_spec_passthrough={"create_temp_table": False},
)


@pytest.mark.parametrize(
    "batch_request,success,value,observed,row_condition",
    [
        (sqlite_runtime_batch_request, True, 100, 100, None),
        (sqlite_batch_request, False, 100, 1313, None),
        (sqlite_batch_request, False, 100, 96, 'col("Age")<18'),
        (sqlite_runtime_batch_request, True, 70, 70, 'col("Age")>17'),
    ],
)
@pytest.mark.slow  # 4.32s
def test_expect_queried_column_value_frequency_to_meet_threshold_sqlite(
    batch_request,
    success,
    value,
    observed,
    row_condition,
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled

    validator: Validator = context.get_validator(batch_request=batch_request)

    result: ExpectationValidationResult = (
        validator.expect_queried_table_row_count_to_be(
            value=value,
            row_condition=row_condition,
            condition_parser="great_expectations__experimental__",
        )
    )

    assert (
        result["success"] is success and result["result"]["observed_value"] == observed
    )


@pytest.mark.parametrize(
    "batch_request,success,query,value,observed,row_condition",
    [
        (
            sqlite_runtime_batch_request,
            True,
            "SELECT COUNT(*) FROM titanic",
            1313,
            1313,
            None,
        ),
        (
            sqlite_batch_request,
            True,
            "SELECT COUNT (*) FROM (SELECT * FROM titanic LIMIT 100)",
            100,
            100,
            'col("Age")>17',
        ),
    ],
)
@pytest.mark.slow  # 1.59s
def test_expect_queried_column_value_frequency_to_meet_threshold_override_query_sqlite(
    batch_request,
    success,
    query,
    value,
    observed,
    row_condition,
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled

    validator: Validator = context.get_validator(batch_request=batch_request)

    result: ExpectationValidationResult = (
        validator.expect_queried_table_row_count_to_be(
            value=value,
            query=query,
            row_condition=row_condition,
            condition_parser="great_expectations__experimental__",
        )
    )

    assert (
        result["success"] is success and result["result"]["observed_value"] == observed
    )


@pytest.mark.parametrize(
    "success,value,observed,row_condition",
    [
        (False, 100, 1313, None),
        (False, 100, 96, 'col("Age")<18'),
    ],
)
def test_expect_queried_column_value_frequency_to_meet_threshold_spark(
    success,
    value,
    observed,
    row_condition,
    spark_session,
    basic_spark_df_execution_engine,
    titanic_df,
):
    df: pd.DataFrame = titanic_df

    validator: Validator = build_spark_validator_with_data(df, spark_session)

    result: ExpectationValidationResult = (
        validator.expect_queried_table_row_count_to_be(
            value=value,
            row_condition=row_condition,
            condition_parser="great_expectations__experimental__",
        )
    )

    assert (
        result["success"] is success and result["result"]["observed_value"] == observed
    )


@pytest.mark.parametrize(
    "success,query,value,observed,row_condition",
    [
        (
            True,
            "SELECT COUNT (*) FROM (SELECT * FROM {active_batch} LIMIT 100)",
            100,
            100,
            'col("Age")>17',
        ),
    ],
)
def test_expect_queried_column_value_frequency_to_meet_threshold_override_query_spark(
    success,
    query,
    value,
    observed,
    row_condition,
    spark_session,
    basic_spark_df_execution_engine,
    titanic_df,
):
    df: pd.DataFrame = titanic_df

    validator: Validator = build_spark_validator_with_data(df, spark_session)

    result: ExpectationValidationResult = (
        validator.expect_queried_table_row_count_to_be(
            value=value,
            query=query,
            row_condition=row_condition,
            condition_parser="great_expectations__experimental__",
        )
    )

    assert (
        result["success"] is success and result["result"]["observed_value"] == observed
    )
