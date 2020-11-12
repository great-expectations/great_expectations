import random
import yaml
import os
import json
import sqlalchemy as sa

from great_expectations.data_context.util import (
    instantiate_class_from_config
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
    random.seed(0)

    db_file = os.path.join(os.getcwd(), "tests", "test_sets", "test_cases_for_sql_data_connector.db")

    #Absolutely minimal starting config
    my_sql_execution_environment = empty_data_context.test_yaml_config(f"""
class_name: StreamlinedSqlExecutionEnvironment
connection_string: sqlite:///{db_file}
""")

    assert my_sql_execution_environment.get_available_data_asset_names() == [
        "foo",
        "bar",
    ]

    # my_data_connector = instantiate_class_from_config(
    #     config,
    #     config_defaults={
    #         "module_name": "great_expectations.execution_environment"
    #     },
    #     runtime_environment={
    #         "name" : "my_sql_execution_environment"
    #     },
    # )

    # report = my_data_connector.self_check()
    # print(json.dumps(report, indent=4))

    # assert report == {
    #     "execution_engine": {
    #         "class_name": "SqlAlchemyExecutionEngine"
    #     },
    #     "data_connectors": {
    #         "count": 1,
    #         "my_snowflake_db": {
    #             "class_name": "SqlDataConnector",
    #             "data_asset_count": 1,
    #             "example_data_asset_names": [
    #                 "table_partitioned_by_date_column__A"
    #             ],
    #             "data_assets": {
    #                 "table_partitioned_by_date_column__A": {
    #                     "batch_definition_count": 5,
    #                     "example_data_references": [
    #                         {
    #                             "date": "2020-00"
    #                         },
    #                         {
    #                             "date": "2020-01"
    #                         },
    #                         {
    #                             "date": "2020-02"
    #                         }
    #                     ]
    #                 }
    #             },
    #             "unmatched_data_reference_count": 0,
    #             "example_unmatched_data_references": [],
    #             "example_data_reference": {
    #                 "batch_spec": {
    #                     "table_name": "table_partitioned_by_date_column__A",
    #                     "partition_definition": {
    #                         "date": "2020-01"
    #                     },
    #                     "splitter_method": "_split_on_converted_datetime",
    #                     "splitter_kwargs": {
    #                         "column_name": "date",
    #                         "date_format_string": "%Y-%W"
    #                     }
    #                 },
    #                 "n_rows": 24
    #             }
    #         }
    #     }
    # }