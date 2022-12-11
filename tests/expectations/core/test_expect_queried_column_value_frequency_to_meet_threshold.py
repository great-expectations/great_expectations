from typing import Optional, cast

import pandas as pd
import pytest

# noinspection PyUnresolvedReferences
from contrib.experimental.great_expectations_experimental.expectations.expect_queried_column_value_frequency_to_meet_threshold import (
    ExpectQueriedColumnValueFrequencyToMeetThreshold,
)
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.self_check.util import (
    build_spark_validator_with_data,
    get_test_validator_with_data,
)
from great_expectations.util import build_in_memory_runtime_context
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
    "batch_request,success,observed,row_condition,warns",
    [
        (sqlite_runtime_batch_request, True, 0.54, None, False),
        (sqlite_batch_request, True, 0.6481340441736482, None, False),
        (sqlite_batch_request, False, 0.4791666666666667, 'col("Age")<18', True),
        (sqlite_runtime_batch_request, True, 0.5, 'col("Age")>17', True),
    ],
)
@pytest.mark.slow  # 3.02s
def test_expect_queried_column_value_frequency_to_meet_threshold_sqlite(
    batch_request,
    success,
    observed,
    row_condition,
    warns,
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled

    validator: Validator = context.get_validator(batch_request=batch_request)

    if warns:
        with pytest.warns(UserWarning):
            result: ExpectationValidationResult = (
                validator.expect_queried_column_value_frequency_to_meet_threshold(
                    column="Sex",
                    value="male",
                    threshold=0.5,
                    row_condition=row_condition,
                    condition_parser="great_expectations__experimental__",
                )
            )
    else:
        result: ExpectationValidationResult = (
            validator.expect_queried_column_value_frequency_to_meet_threshold(
                column="Sex",
                value="male",
                threshold=0.5,
                row_condition=row_condition,
                condition_parser="great_expectations__experimental__",
            )
        )
    assert (
        result["success"] == success and result["result"]["observed_value"] == observed
    )


@pytest.mark.parametrize(
    "batch_request,query,success,observed,row_condition,warns",
    [
        (
            sqlite_batch_request,
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM titanic) FROM titanic GROUP BY {col}",
            True,
            0.6481340441736482,
            None,
            True,
        ),
        (
            sqlite_runtime_batch_request,
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM titanic) FROM {active_batch} GROUP BY {col}",
            False,
            0.04112718964204113,
            None,
            True,
        ),
        (
            sqlite_batch_request,
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT(y) FROM wrong) FROM {active_batch} GROUP BY {col}",
            True,
            7.091666666666667,
            None,
            True,
        ),
        (
            sqlite_batch_request,
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM titanic) FROM {active_batch} GROUP BY {col}",
            False,
            0.2338156892612338,
            'col("Age")<35',
            True,
        ),
        (
            sqlite_batch_request,
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM {active_batch}) / 2 FROM {active_batch} GROUP BY {col}",
            False,
            0.3240670220868241,
            None,
            False,
        ),
    ],
)
@pytest.mark.slow  # 3.92s
def test_expect_queried_column_value_frequency_to_meet_threshold_override_query_sqlite(
    batch_request,
    query,
    success,
    observed,
    row_condition,
    warns,
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled

    validator: Validator = context.get_validator(batch_request=batch_request)

    if warns:
        with pytest.warns(UserWarning):
            result: ExpectationValidationResult = (
                validator.expect_queried_column_value_frequency_to_meet_threshold(
                    column="Sex",
                    value="male",
                    threshold=0.5,
                    query=query,
                    row_condition=row_condition,
                    condition_parser="great_expectations__experimental__",
                )
            )
    else:
        result: ExpectationValidationResult = (
            validator.expect_queried_column_value_frequency_to_meet_threshold(
                column="Sex",
                value="male",
                threshold=0.5,
                query=query,
                row_condition=row_condition,
                condition_parser="great_expectations__experimental__",
            )
        )
    assert (
        result["success"] == success and result["result"]["observed_value"] == observed
    )


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "success,observed,row_condition,warns",
    [
        (True, 0.6481340441736482, None, False),
        (False, 0.4791666666666667, 'col("Age")<18', True),
        (True, 0.6614626129827444, 'col("Age")>17', True),
    ],
)
def test_expect_queried_column_value_frequency_to_meet_threshold_spark(
    success,
    observed,
    row_condition,
    warns,
    spark_session,
    basic_spark_df_execution_engine,
    titanic_df,
):
    df: pd.DataFrame = titanic_df

    context: Optional[DataContext] = cast(
        DataContext, build_in_memory_runtime_context()
    )
    validator = get_test_validator_with_data(
        execution_engine="spark",
        data=df,
        context=context,
    )

    if warns:
        with pytest.warns(UserWarning):
            result: ExpectationValidationResult = (
                validator.expect_queried_column_value_frequency_to_meet_threshold(
                    column="Sex",
                    value="male",
                    threshold=0.5,
                    row_condition=row_condition,
                    condition_parser="great_expectations__experimental__",
                )
            )
    else:
        result: ExpectationValidationResult = (
            validator.expect_queried_column_value_frequency_to_meet_threshold(
                column="Sex",
                value="male",
                threshold=0.5,
                row_condition=row_condition,
                condition_parser="great_expectations__experimental__",
            )
        )
    assert (
        result["success"] == success and result["result"]["observed_value"] == observed
    )


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "query,success,observed,row_condition,warns",
    [
        (
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM {active_batch}) / 2 FROM {active_batch} GROUP BY {col}",
            False,
            0.3240670220868241,
            None,
            False,
        ),
        (
            "SELECT {col}, CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM {active_batch}) / 2 FROM {active_batch} GROUP BY {col}",
            False,
            0.3107287449392713,
            'col("Age")<35',
            True,
        ),
    ],
)
def test_expect_queried_column_value_frequency_to_meet_threshold_override_query_spark(
    query,
    success,
    observed,
    row_condition,
    warns,
    spark_session,
    basic_spark_df_execution_engine,
    titanic_df,
):
    df: pd.DataFrame = titanic_df

    context: Optional[DataContext] = cast(
        DataContext, build_in_memory_runtime_context()
    )
    validator = get_test_validator_with_data(
        execution_engine="spark",
        data=df,
        context=context,
    )

    if warns:
        with pytest.warns(UserWarning):
            result: ExpectationValidationResult = (
                validator.expect_queried_column_value_frequency_to_meet_threshold(
                    column="Sex",
                    value="male",
                    threshold=0.5,
                    query=query,
                    row_condition=row_condition,
                    condition_parser="great_expectations__experimental__",
                )
            )
    else:
        result: ExpectationValidationResult = (
            validator.expect_queried_column_value_frequency_to_meet_threshold(
                column="Sex",
                value="male",
                threshold=0.5,
                query=query,
                row_condition=row_condition,
                condition_parser="great_expectations__experimental__",
            )
        )
    assert (
        result["success"] == success and result["result"]["observed_value"] == observed
    )


def test_expect_queried_column_value_frequency_to_meet_threshold_sqlite_multi_value(
    titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_pandas_and_sqlalchemy_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled

    validator: Validator = context.get_validator(batch_request=sqlite_batch_request)

    with pytest.warns(UserWarning):
        result: ExpectationValidationResult = (
            validator.expect_queried_column_value_frequency_to_meet_threshold(
                column="Sex",
                value=["male", "female"],
                threshold=[0.6, 0.3],
                row_condition='col("Age")>17',
                condition_parser="great_expectations__experimental__",
            )
        )

    assert result["success"] == True and result["result"]["observed_value"] == [
        0.6393939393939394,
        0.3606060606060606,
    ]
