import pandas as pd
import pytest

import great_expectations.exceptions.exceptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.self_check.util import (
    build_sa_validator_with_data,
    build_spark_validator_with_data,
)
from tests.integration.docusaurus.expectations.creating_custom_expectations.expect_queried_column_value_frequency_to_meet_threshold import (
    ExpectQueriedColumnValueFrequencyToMeetThreshold,
)


def test_parameterized_expect_table_query_column_value_frequency_to_meet_threshold(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request = RuntimeBatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="titanic",
        runtime_parameters={"query": "SELECT * FROM titanic"},
        batch_identifiers={"default_identifier_name": "test_identifier"},
        batch_spec_passthrough={"create_temp_table": False},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_queried_column_value_frequency_to_meet_threshold(
        query="SELECT Sex, "
        "ROUND(CAST(COUNT(Sex) as float) / (SELECT COUNT(Sex) FROM titanic), 2) "
        "FROM titanic "
        "GROUP BY Sex",
        value="male",
        threshold=0.5,
    )

    assert result["success"] is True and result["result"]["observed_value"] == 0.65
