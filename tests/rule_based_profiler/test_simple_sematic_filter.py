from __future__ import annotations

import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.domain import SemanticDomainTypes
from great_expectations.data_context.util import file_relative_path
from great_expectations.rule_based_profiler.helpers.simple_semantic_type_filter import (
    SimpleSemanticTypeFilter,
)
from great_expectations.rule_based_profiler.helpers.util import get_batch_ids
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.test_utils import load_data_into_test_database


@pytest.mark.trino
def test_simple_sematic_filter_defaults_to_all_columns(empty_data_context):
    CONNECTION_STRING = "trino://test@localhost:8088/memory/schema"

    # This utility is not for general use. It is only to support testing.
    load_data_into_test_database(
        table_name="taxi_data",
        csv_path=file_relative_path(
            __file__,
            "../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
        ),
        connection_string=CONNECTION_STRING,
        convert_colnames_to_datetime=["pickup_datetime"],
    )

    context = empty_data_context
    datasource_config = {
        "name": "my_trino_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
            },
        },
    }

    context.add_datasource(**datasource_config)
    batch_request = RuntimeBatchRequest(
        datasource_name="my_trino_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="default_name",  # this can be anything that identifies this data
        runtime_parameters={
            "query": "SELECT pickup_datetime, dropoff_datetime, store_and_fwd_flag from taxi_data LIMIT 10"
        },
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )

    desired_metric = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    validator.execution_engine.resolve_metrics([desired_metric])

    batch_ids = get_batch_ids(data_context=context, batch_request=batch_request)
    # Ensure that the data types are read correctly with column_names listed
    semantic_type_filter = SimpleSemanticTypeFilter(
        validator=validator,
        batch_ids=batch_ids,
        column_names=["pickup_datetime", "dropoff_datetime", "store_and_fwd_flag"],
    )
    assert (
        semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map
        == {
            "pickup_datetime": SemanticDomainTypes.DATETIME,
            "dropoff_datetime": SemanticDomainTypes.TEXT,
            "store_and_fwd_flag": SemanticDomainTypes.TEXT,
        }
    )

    # Ensure that the data types are read correctly without column_names listed
    semantic_type_filter = SimpleSemanticTypeFilter(
        validator=validator,
        batch_ids=batch_ids,
    )

    semantic_column_types = (
        semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map
    )
    assert semantic_column_types["pickup_datetime"] == SemanticDomainTypes.DATETIME
    assert semantic_column_types["dropoff_datetime"] == SemanticDomainTypes.TEXT
