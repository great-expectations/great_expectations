import pytest
import os
import yaml
import json
import sqlalchemy

from great_expectations.data_context.util import file_relative_path
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
    PartitionRequest,
)

@pytest.fixture
def data_context_with_sql_execution_environment_for_testing_get_batch(empty_data_context):
    db_file = file_relative_path(
        __file__,
        "../test_sets/test_cases_for_sql_data_connector.db",
    )

    config = yaml.load(
        f"""
class_name: StreamlinedSqlExecutionEnvironment
connection_string: sqlite:///{db_file}
"""+"""
introspection:
    whole_table: {}

    daily:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"

    weekly:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%W"

    by_id_dozens:
        splitter_method: _split_on_divided_integer
        splitter_kwargs:
            column_name: id
            divisor: 12
""",
        yaml.FullLoader,
    )

    empty_data_context.add_execution_environment(
        "my_sqlite_db",
        config
    )

    return empty_data_context



def test_get_batch(data_context_with_sql_execution_environment_for_testing_get_batch):
    context = data_context_with_sql_execution_environment_for_testing_get_batch

    print(json.dumps(context.datasources["my_sqlite_db"].get_available_data_asset_names(), indent=4))

    # Successful specification using a BatchDefinition
    context.get_batch_from_new_style_datasource(
        batch_definition=BatchDefinition(
            execution_environment_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A__daily",
            partition_definition=PartitionDefinition(
                date="2020-01-15",
            )
        )
    )

    # Failed specification using a mistyped batch_definition
    with pytest.raises(TypeError):
        context.get_batch_from_new_style_datasource(
            batch_definition=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A__daily",
                partition_request=PartitionRequest(
                    partition_identifiers={
                        "date": "2020-01-15"
                    }
                )
            )
        )

    # Successful specification using a typed BatchRequest
    context.get_batch_from_new_style_datasource(
        batch_request=BatchRequest(
            execution_environment_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A__daily",
            partition_request=PartitionRequest(
                partition_identifiers={
                    "date": "2020-01-15"
                }
            )
        )
    )

    # Failed specification using an untyped BatchRequest
    with pytest.raises(AttributeError):
        context.get_batch_from_new_style_datasource(
            batch_request={
                "execution_environment_name" : "my_sqlite_db",
                "data_connector_name" : "daily",
                "data_asset_name" : "table_partitioned_by_date_column__A__daily",
                "partition_request" : {
                    "partition_identifiers" : {
                        "date": "2020-01-15"
                    }
                }
            }
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(ValueError):
        context.get_batch_from_new_style_datasource(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A__daily",
                partition_request=PartitionRequest(
                    partition_identifiers={}
                )
            )
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(ValueError):
        context.get_batch_from_new_style_datasource(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A__daily",
            )
        )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(KeyError):
        context.get_batch_from_new_style_datasource(
            batch_request=BatchRequest(
                execution_environment_name="my_sqlite_db",
                data_connector_name="daily",
            )
        )

    # Failed specification using an incomplete BatchRequest
    # with pytest.raises(ValueError):
    with pytest.raises(KeyError):
        context.get_batch_from_new_style_datasource(
            batch_request=BatchRequest(
                # execution_environment_name=MISSING
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A__daily",
                partition_request=PartitionRequest(
                    partition_identifiers={}
                )
            )
        )

    # Successful specification using parameters
    context.get_batch_from_new_style_datasource(
        execution_environment_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A__daily",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # This is the thinnest this can plausibly get.
    context.get_batch_from_new_style_datasource(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A__daily",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # In the case of a data_asset containing a single Batch, we don't even need parameters
    context.get_batch_from_new_style_datasource(
        "my_sqlite_db",
        "whole_table",
        "table_partitioned_by_date_column__A__whole_table",
    )

    # Successful specification using parameters and partition_request
    context.get_batch_from_new_style_datasource(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A__daily",
        partition_request=PartitionRequest({
            "partition_identifiers": {
                "date": "2020-01-15"
            }
        })
    )

    # Successful specification using parameters and partition_identifiers
    context.get_batch_from_new_style_datasource(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A__daily",
        partition_identifiers={
            "date": "2020-01-15"
        }
    )


def test_get_batch_list_from_new_style_datasource(
    data_context_with_sql_execution_environment_for_testing_get_batch
):
    context = data_context_with_sql_execution_environment_for_testing_get_batch
    
    batch_list = context.get_batch_list_from_new_style_datasource({
        "execution_environment_name" : "my_sqlite_db",
        "data_connector_name": "daily",
        "data_asset_name": "table_partitioned_by_date_column__A__daily",
        "partition_request" : {
            "partition_identifiers" : {
                "date": "2020-01-15"
            }
        }
    })

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]

    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "table_partitioned_by_date_column__A__daily"
    assert batch.batch_definition["partition_definition"] == {
        "date": "2020-01-15"
    }
    assert isinstance(batch.data, sqlalchemy.engine.result.ResultProxy)
    assert len(batch.data.fetchall()) == 4
