import random
import yaml
import os
import json
import sqlalchemy as sa

from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from great_expectations.core.batch import (
    BatchRequest
)

def test_basic_instantiation():
    random.seed(0)

    db_file = os.path.join(os.getcwd(), "tests", "test_sets", "test_cases_for_sql_data_connector.db")

    # This is a basic intergration test demonstrating an ExecutionEnvironment containing a SQL data_connector
    # It also shows how to instantiate a SQLite SqlAlchemyExecutionEngine
    config = yaml.load(
        f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: sqlite:///{db_file}

data_connectors:
    my_snowflake_db:
        class_name: SqlDataConnector

        data_assets:

            table_partitioned_by_date_column__A:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
    """,
        yaml.FullLoader,
    )

    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={
            "module_name": "great_expectations.execution_environment"
        },
        runtime_environment={
            "name" : "my_sql_execution_environment"
        },
    )

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=4))

    assert report == {
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine"
        },
        "data_connectors": {
            "count": 1,
            "my_snowflake_db": {
                "class_name": "SqlDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": [
                    "table_partitioned_by_date_column__A"
                ],
                "data_assets": {
                    "table_partitioned_by_date_column__A": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {
                                "date": "2020-00"
                            },
                            {
                                "date": "2020-01"
                            },
                            {
                                "date": "2020-02"
                            }
                        ]
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                "example_data_reference": {
                    "batch_spec": {
                        "table_name": "table_partitioned_by_date_column__A",
                        "partition_definition": {
                            "date": "2020-01"
                        },
                        "splitter_method": "_split_on_converted_datetime",
                        "splitter_kwargs": {
                            "column_name": "date",
                            "date_format_string": "%Y-%W"
                        }
                    },
                    "n_rows": 24
                }
            }
        }
    }

def test_StreamlinedSqlExecutionEnvironment(empty_data_context):
    # This test mirrors the likely path to configure a StreamlinedSqlExecutionEnvironment
    db_file = os.path.join(os.getcwd(), "tests", "test_sets", "test_cases_for_sql_data_connector.db")

    #Absolutely minimal starting config
    my_sql_execution_environment = empty_data_context.test_yaml_config(f"""
class_name: StreamlinedSqlExecutionEnvironment
connection_string: sqlite:///{db_file}
""")

    print(my_sql_execution_environment.get_available_data_asset_names())
    assert my_sql_execution_environment.get_available_data_asset_names() == {
        "ONLY_DATA_CONNECTOR": [
            "main.table_containing_id_spacers_for_D__whole",
            "main.table_partitioned_by_date_column__A__whole",
            "main.table_partitioned_by_foreign_key__F__whole",
            "main.table_partitioned_by_incrementing_batch_id__E__whole",
            "main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
            "main.table_partitioned_by_multiple_columns__G__whole",
            "main.table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole",
            "main.table_partitioned_by_timestamp_column__B__whole",
            "main.table_that_should_be_partitioned_by_random_hash__H__whole",
            "main.table_with_fk_reference_from_F__whole",
        ]
    }


#Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_introspect_db(test_cases_for_sql_data_connector_sqlite_execution_engine):
    #Note: Abe 2020111: this test currently only uses a sqlite fixture.
    # We should extend this to at least include postgresql in the unit tests.
    # Other DBs can be run as integration tests.
    
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "SqlDataConnector",
            "name": "my_test_data_connector",
            "data_assets": {},
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "execution_environment_name": "my_text_execution_environment",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    print(my_data_connector._introspect_db())
    assert my_data_connector._introspect_db() == [
        {"schema_name": "main", "table_name": "table_containing_id_spacers_for_D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_date_column__A", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_foreign_key__F", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_incrementing_batch_id__E", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_multiple_columns__G", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_timestamp_column__B", "type": "table"},
        {"schema_name": "main", "table_name": "table_that_should_be_partitioned_by_random_hash__H", "type": "table"},
        {"schema_name": "main", "table_name": "table_with_fk_reference_from_F", "type": "table"},
    ]

    assert my_data_connector._introspect_db(
        schema_name="main"
    ) == [
        {"schema_name": "main", "table_name": "table_containing_id_spacers_for_D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_date_column__A", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_foreign_key__F", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_incrementing_batch_id__E", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_multiple_columns__G", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_timestamp_column__B", "type": "table"},
        {"schema_name": "main", "table_name": "table_that_should_be_partitioned_by_random_hash__H", "type": "table"},
        {"schema_name": "main", "table_name": "table_with_fk_reference_from_F", "type": "table"},
    ]

    assert my_data_connector._introspect_db(
        schema_name="waffle"
    ) == []

    # This is a weak test, since this db doesn't have any additional schemas or system tables to show.
    assert my_data_connector._introspect_db(
        ignore_information_schemas_and_system_tables=False
    ) == [
        {"schema_name": "main", "table_name": "table_containing_id_spacers_for_D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_date_column__A", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_foreign_key__F", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_incrementing_batch_id__E", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_multiple_columns__G", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C", "type": "table"},
        {"schema_name": "main", "table_name": "table_partitioned_by_timestamp_column__B", "type": "table"},
        {"schema_name": "main", "table_name": "table_that_should_be_partitioned_by_random_hash__H", "type": "table"},
        {"schema_name": "main", "table_name": "table_with_fk_reference_from_F", "type": "table"},
    ]

    #Need to test include_views, too.

#Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_include_introspected_whole_tables_as_data_assets(test_cases_for_sql_data_connector_sqlite_execution_engine):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "SqlDataConnector",
            "name": "my_test_data_connector",
            "data_assets": {},
            "include_introspected_whole_tables_as_data_assets": True,
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "execution_environment_name": "my_test_execution_environment",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    assert my_data_connector.get_available_data_asset_names() == [
        "main.table_containing_id_spacers_for_D__whole",
        "main.table_partitioned_by_date_column__A__whole",
        "main.table_partitioned_by_foreign_key__F__whole",
        "main.table_partitioned_by_incrementing_batch_id__E__whole",
        "main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.table_partitioned_by_multiple_columns__G__whole",
        "main.table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.table_partitioned_by_timestamp_column__B__whole",
        "main.table_that_should_be_partitioned_by_random_hash__H__whole",
        "main.table_with_fk_reference_from_F__whole",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="my_test_execution_environment",
        data_connector_name="my_test_data_connector",
        data_asset_name="main.table_that_should_be_partitioned_by_random_hash__H__whole",
    ))
    assert len(batch_definition_list) == 1
