import json

import pytest
import yaml

from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.data_context.util import file_relative_path


def test_get_batch(data_context_with_sql_execution_environment_for_testing_get_batch):
    context = data_context_with_sql_execution_environment_for_testing_get_batch

    print(
        json.dumps(
            context.datasources["my_sqlite_db"].get_available_data_asset_names(),
            indent=4,
        )
    )

    # Successful specification using a BatchDefinition
    context.get_batch(
        batch_definition=BatchDefinition(
            execution_environment_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            partition_definition=PartitionDefinition(date="2020-01-15",),
        )
    )

    # Failed specification using a mistyped batch_definition
    with pytest.raises(TypeError):
        context.get_batch(
            batch_definition=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request=PartitionRequest(
                    partition_identifiers={"date": "2020-01-15"}
                ),
            )
        )

    # Successful specification using a typed BatchRequest
    context.get_batch(
        batch_request=BatchRequest(
            execution_environment_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            partition_request=PartitionRequest(
                partition_identifiers={"date": "2020-01-15"}
            ),
        )
    )

    # Failed specification using an untyped BatchRequest
    with pytest.raises(AttributeError):
        context.get_batch(
            batch_request={
                "execution_environment_name": "my_sqlite_db",
                "data_connector_name": "daily",
                "data_asset_name": "table_partitioned_by_date_column__A",
                "partition_request": {"partition_identifiers": {"date": "2020-01-15"}},
            }
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(ValueError):
        context.get_batch(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request=PartitionRequest(partition_identifiers={}),
            )
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(ValueError):
        context.get_batch(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
            )
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(KeyError):
        context.get_batch(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db", data_connector_name="daily",
            )
        )

    # Failed specification using an incomplete BatchRequest
    # with pytest.raises(ValueError):
    with pytest.raises(KeyError):
        context.get_batch(
            batch_request=BatchRequest(
                # execution_environment_name=MISSING
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
                partition_request=PartitionRequest(partition_identifiers={}),
            )
        )

    # Successful specification using parameters
    context.get_batch(
        execution_environment_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # This is the thinnest this can plausibly get.
    context.get_batch(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # In the case of a data_asset containing a single Batch, we don't even need parameters
    context.get_batch(
        "my_sqlite_db", "whole_table", "table_partitioned_by_date_column__A",
    )

    # Successful specification using parameters and partition_request
    context.get_batch(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        partition_request=PartitionRequest(
            {"partition_identifiers": {"date": "2020-01-15"}}
        ),
    )

    # Successful specification using parameters and partition_identifiers
    context.get_batch(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        partition_identifiers={"date": "2020-01-15"},
    )


def test_get_batch_list_from_new_style_datasource_with_sql_execution_environment(
    sa, data_context_with_sql_execution_environment_for_testing_get_batch
):
    context = data_context_with_sql_execution_environment_for_testing_get_batch

    batch_list = context.get_batch_list_from_new_style_datasource(
        {
            "execution_environment_name": "my_sqlite_db",
            "data_connector_name": "daily",
            "data_asset_name": "table_partitioned_by_date_column__A",
            "partition_request": {"partition_identifiers": {"date": "2020-01-15"}},
        }
    )

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert (
        batch.batch_definition["data_asset_name"]
        == "table_partitioned_by_date_column__A"
    )
    assert batch.batch_definition["partition_definition"] == {"date": "2020-01-15"}
    assert isinstance(batch.data, sa.engine.result.ResultProxy)
    assert len(batch.data.fetchall()) == 4
