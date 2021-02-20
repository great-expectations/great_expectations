import json
import os
import random

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

yaml = YAML()


def test_basic_instantiation(sa):
    random.seed(0)

    db_file = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    # This is a basic integration test demonstrating an Datasource containing a SQL data_connector
    # It also shows how to instantiate a SQLite SqlAlchemyExecutionEngine
    config = yaml.load(
        f"""
class_name: Datasource

execution_engine:
    class_name: SqlAlchemyExecutionEngine
    connection_string: sqlite:///{db_file}

data_connectors:
    my_sqlite_db:
        class_name: ConfiguredAssetSqlDataConnector

        data_assets:

            table_partitioned_by_date_column__A:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
    """,
    )

    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_sql_datasource"},
    )

    report = my_data_connector.self_check()
    # print(json.dumps(report, indent=4))

    report["execution_engine"].pop("connection_string")

    assert report == {
        "execution_engine": {
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "data_asset_count": 1,
                "example_data_asset_names": ["table_partitioned_by_date_column__A"],
                "data_assets": {
                    "table_partitioned_by_date_column__A": {
                        "batch_definition_count": 5,
                        "example_data_references": [
                            {"date": "2020-00"},
                            {"date": "2020-01"},
                            {"date": "2020-02"},
                        ],
                    }
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
                "example_data_reference": {
                    "batch_spec": {
                        "table_name": "table_partitioned_by_date_column__A",
                        "partition_definition": {"date": "2020-01"},
                        "splitter_method": "_split_on_converted_datetime",
                        "splitter_kwargs": {
                            "column_name": "date",
                            "date_format_string": "%Y-%W",
                        },
                    },
                    "n_rows": 24,
                },
            },
        },
    }


def test_SimpleSqlalchemyDatasource(empty_data_context):
    context = empty_data_context
    # This test mirrors the likely path to configure a SimpleSqlalchemyDatasource

    db_file = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    # Absolutely minimal starting config
    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table: {}
"""
    )
    print(json.dumps(my_sql_datasource.get_available_data_asset_names(), indent=4))

    assert my_sql_datasource.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_partitioned_by_foreign_key__F",
            "table_partitioned_by_incrementing_batch_id__E",
            "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "table_partitioned_by_multiple_columns__G",
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "table_partitioned_by_timestamp_column__B",
            "table_that_should_be_partitioned_by_random_hash__H",
            "table_with_fk_reference_from_F",
            "view_by_date_column__A",
            "view_by_incrementing_batch_id__E",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "view_by_multiple_columns__G",
            "view_by_regularly_spaced_incrementing_id_column__C",
            "view_by_timestamp_column__B",
            "view_containing_id_spacers_for_D",
            "view_partitioned_by_foreign_key__F",
            "view_that_should_be_partitioned_by_random_hash__H",
            "view_with_fk_reference_from_F",
        ]
    }

    # Here we should test getting a batch

    # Very thin starting config
    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table:
        data_asset_name_suffix: __whole_table
        introspection_directives: {}
"""
    )

    assert my_sql_datasource.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D__whole_table",
            "table_full__I__whole_table",
            "table_partitioned_by_date_column__A__whole_table",
            "table_partitioned_by_foreign_key__F__whole_table",
            "table_partitioned_by_incrementing_batch_id__E__whole_table",
            "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole_table",
            "table_partitioned_by_multiple_columns__G__whole_table",
            "table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole_table",
            "table_partitioned_by_timestamp_column__B__whole_table",
            "table_that_should_be_partitioned_by_random_hash__H__whole_table",
            "table_with_fk_reference_from_F__whole_table",
            "view_by_date_column__A__whole_table",
            "view_by_incrementing_batch_id__E__whole_table",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole_table",
            "view_by_multiple_columns__G__whole_table",
            "view_by_regularly_spaced_incrementing_id_column__C__whole_table",
            "view_by_timestamp_column__B__whole_table",
            "view_containing_id_spacers_for_D__whole_table",
            "view_partitioned_by_foreign_key__F__whole_table",
            "view_that_should_be_partitioned_by_random_hash__H__whole_table",
            "view_with_fk_reference_from_F__whole_table",
        ]
    }

    # Here we should test getting a batch

    # Add some manually configured tables...
    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}

introspection:
    whole_table:
        excluded_tables:
            - main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D
            - main.table_partitioned_by_multiple_columns__G
            - main.table_partitioned_by_regularly_spaced_incrementing_id_column__C
            - main.table_partitioned_by_timestamp_column__B
            - main.table_that_should_be_partitioned_by_random_hash__H
            - main.table_with_fk_reference_from_F

    hourly:
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: timestamp
            date_format_string: "%Y-%m-%d:%H"
        included_tables:
            - main.table_partitioned_by_timestamp_column__B
        introspection_directives:
            include_views: true


tables:
    table_partitioned_by_date_column__A:
        partitioners:
            daily:
                data_asset_name_suffix: __daily
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%m-%d"
            weekly:
                include_schema_name: False
                data_asset_name_prefix: some_string__
                data_asset_name_suffix: __some_other_string
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
            by_id_dozens:
                include_schema_name: True
                # Note: no data_asset_name_suffix
                splitter_method: _split_on_divided_integer
                splitter_kwargs:
                    column_name: id
                    divisor: 12
"""
    )

    print(json.dumps(my_sql_datasource.get_available_data_asset_names(), indent=4))
    assert my_sql_datasource.get_available_data_asset_names() == {
        "whole_table": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_partitioned_by_foreign_key__F",
            "table_partitioned_by_incrementing_batch_id__E",
            "view_by_date_column__A",
            "view_by_incrementing_batch_id__E",
            "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "view_by_multiple_columns__G",
            "view_by_regularly_spaced_incrementing_id_column__C",
            "view_by_timestamp_column__B",
            "view_containing_id_spacers_for_D",
            "view_partitioned_by_foreign_key__F",
            "view_that_should_be_partitioned_by_random_hash__H",
            "view_with_fk_reference_from_F",
        ],
        "hourly": [
            "table_partitioned_by_timestamp_column__B",
        ],
        "daily": [
            "table_partitioned_by_date_column__A__daily",
        ],
        "weekly": [
            "some_string__table_partitioned_by_date_column__A__some_other_string",
        ],
        "by_id_dozens": [
            "table_partitioned_by_date_column__A",
        ],
    }

    # Here we should test getting another batch

    # Drop the introspection...
    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
tables:
    table_partitioned_by_date_column__A:
        partitioners:
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
"""
    )
    print(json.dumps(my_sql_datasource.get_available_data_asset_names(), indent=4))
    assert my_sql_datasource.get_available_data_asset_names() == {
        "whole_table": [
            "table_partitioned_by_date_column__A",
        ],
        "daily": [
            "table_partitioned_by_date_column__A",
        ],
        "weekly": [
            "table_partitioned_by_date_column__A",
        ],
        "by_id_dozens": [
            "table_partitioned_by_date_column__A",
        ],
    }

    # Here we should test getting another batch


# Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_introspect_db(test_cases_for_sql_data_connector_sqlite_execution_engine):
    # Note: Abe 2020111: this test currently only uses a sqlite fixture.
    # We should extend this to at least include postgresql in the unit tests.
    # Other DBs can be run as integration tests.

    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "my_test_data_connector",
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    # print(my_data_connector._introspect_db())
    assert my_data_connector._introspect_db() == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]

    assert my_data_connector._introspect_db(schema_name="main") == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]

    assert my_data_connector._introspect_db(schema_name="waffle") == []

    # This is a weak test, since this db doesn't have any additional schemas or system tables to show.
    assert my_data_connector._introspect_db(
        ignore_information_schemas_and_system_tables=False
    ) == [
        {
            "schema_name": "main",
            "table_name": "table_containing_id_spacers_for_D",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "table_full__I", "type": "table"},
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_date_column__A",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_foreign_key__F",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_incrementing_batch_id__E",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_multiple_columns__G",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_partitioned_by_timestamp_column__B",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_that_should_be_partitioned_by_random_hash__H",
            "type": "table",
        },
        {
            "schema_name": "main",
            "table_name": "table_with_fk_reference_from_F",
            "type": "table",
        },
        {"schema_name": "main", "table_name": "view_by_date_column__A", "type": "view"},
        {
            "schema_name": "main",
            "table_name": "view_by_incrementing_batch_id__E",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_multiple_columns__G",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_regularly_spaced_incrementing_id_column__C",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_by_timestamp_column__B",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_containing_id_spacers_for_D",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_partitioned_by_foreign_key__F",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_that_should_be_partitioned_by_random_hash__H",
            "type": "view",
        },
        {
            "schema_name": "main",
            "table_name": "view_with_fk_reference_from_F",
            "type": "view",
        },
    ]


# Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_basic_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_prefix": "prexif__",
            "data_asset_name_suffix": "__xiffus",
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    report_object = my_data_connector.self_check()
    # print(json.dumps(report_object, indent=4))
    assert report_object == {
        "class_name": "InferredAssetSqlDataConnector",
        "data_asset_count": 21,
        "example_data_asset_names": [
            "prexif__table_containing_id_spacers_for_D__xiffus",
            "prexif__table_full__I__xiffus",
            "prexif__table_partitioned_by_date_column__A__xiffus",
        ],
        "data_assets": {
            "prexif__table_containing_id_spacers_for_D__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prexif__table_full__I__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "prexif__table_partitioned_by_date_column__A__xiffus": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "batch_spec": {
                "schema_name": "main",
                "table_name": "table_containing_id_spacers_for_D",
                "partition_definition": {},
            },
            "n_rows": 30,
        },
    }

    assert my_data_connector.get_available_data_asset_names() == [
        "prexif__table_containing_id_spacers_for_D__xiffus",
        "prexif__table_full__I__xiffus",
        "prexif__table_partitioned_by_date_column__A__xiffus",
        "prexif__table_partitioned_by_foreign_key__F__xiffus",
        "prexif__table_partitioned_by_incrementing_batch_id__E__xiffus",
        "prexif__table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__table_partitioned_by_multiple_columns__G__xiffus",
        "prexif__table_partitioned_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__table_partitioned_by_timestamp_column__B__xiffus",
        "prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__table_with_fk_reference_from_F__xiffus",
        "prexif__view_by_date_column__A__xiffus",
        "prexif__view_by_incrementing_batch_id__E__xiffus",
        "prexif__view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__xiffus",
        "prexif__view_by_multiple_columns__G__xiffus",
        "prexif__view_by_regularly_spaced_incrementing_id_column__C__xiffus",
        "prexif__view_by_timestamp_column__B__xiffus",
        "prexif__view_containing_id_spacers_for_D__xiffus",
        "prexif__view_partitioned_by_foreign_key__F__xiffus",
        "prexif__view_that_should_be_partitioned_by_random_hash__H__xiffus",
        "prexif__view_with_fk_reference_from_F__xiffus",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="prexif__table_that_should_be_partitioned_by_random_hash__H__xiffus",
        )
    )
    assert len(batch_definition_list) == 1


# Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_more_complex_instantiation_of_InferredAssetSqlDataConnector(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "data_asset_name_suffix": "__whole",
            "include_schema_name": True,
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_sqlite_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    report_object = my_data_connector.self_check()

    assert report_object == {
        "class_name": "InferredAssetSqlDataConnector",
        "data_asset_count": 21,
        "data_assets": {
            "main.table_containing_id_spacers_for_D__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "main.table_full__I__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "main.table_partitioned_by_date_column__A__whole": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "example_data_asset_names": [
            "main.table_containing_id_spacers_for_D__whole",
            "main.table_full__I__whole",
            "main.table_partitioned_by_date_column__A__whole",
        ],
        "example_data_reference": {
            "batch_spec": {
                "partition_definition": {},
                "schema_name": "main",
                "table_name": "table_containing_id_spacers_for_D",
            },
            "n_rows": 30,
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    assert my_data_connector.get_available_data_asset_names() == [
        "main.table_containing_id_spacers_for_D__whole",
        "main.table_full__I__whole",
        "main.table_partitioned_by_date_column__A__whole",
        "main.table_partitioned_by_foreign_key__F__whole",
        "main.table_partitioned_by_incrementing_batch_id__E__whole",
        "main.table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.table_partitioned_by_multiple_columns__G__whole",
        "main.table_partitioned_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.table_partitioned_by_timestamp_column__B__whole",
        "main.table_that_should_be_partitioned_by_random_hash__H__whole",
        "main.table_with_fk_reference_from_F__whole",
        "main.view_by_date_column__A__whole",
        "main.view_by_incrementing_batch_id__E__whole",
        "main.view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D__whole",
        "main.view_by_multiple_columns__G__whole",
        "main.view_by_regularly_spaced_incrementing_id_column__C__whole",
        "main.view_by_timestamp_column__B__whole",
        "main.view_containing_id_spacers_for_D__whole",
        "main.view_partitioned_by_foreign_key__F__whole",
        "main.view_that_should_be_partitioned_by_random_hash__H__whole",
        "main.view_with_fk_reference_from_F__whole",
    ]

    batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="my_test_datasource",
            data_connector_name="whole_table",
            data_asset_name="main.table_that_should_be_partitioned_by_random_hash__H__whole",
        )
    )
    assert len(batch_definition_list) == 1


# Note: Abe 2020111: this test belongs with the data_connector tests, not here.
def test_more_complex_instantiation_of_InferredAssetSqlDataConnector_postgres_backend(
    test_cases_for_sql_data_connector_postgres_execution_engine,
):
    my_data_connector = instantiate_class_from_config(
        config={
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": True,
        },
        runtime_environment={
            "execution_engine": test_cases_for_sql_data_connector_postgres_execution_engine,
            "datasource_name": "my_test_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    report_object = my_data_connector.self_check()
    assert report_object == {
        "class_name": "InferredAssetSqlDataConnector",
        "data_asset_count": 115,
        "example_data_asset_names": [
            "public.expect_table_row_count_to_equal_other_table_data_1",
            "public.expect_table_row_count_to_equal_other_table_data_2",
            "public.expect_table_row_count_to_equal_other_table_data_3",
        ],
        "data_assets": {
            "public.expect_table_row_count_to_equal_other_table_data_1": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "public.expect_table_row_count_to_equal_other_table_data_2": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
            "public.expect_table_row_count_to_equal_other_table_data_3": {
                "batch_definition_count": 1,
                "example_data_references": [{}],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {
            "batch_spec": {
                "table_name": "expect_table_row_count_to_equal_other_table_data_1",
                "partition_definition": {},
                "schema_name": "public",
            },
            "n_rows": 4,
        },
    }

    assert my_data_connector.get_available_data_asset_names() == [
        "public.test_data_ZBLTfcIS",
        "public.test_data_SD9pH3cX",
        "public.test_data_EzzgDDIu",
        "public.test_data_VkGYJ6VM",
        "public.test_data_5NYbJF88",
        "public.test_data_5RL1Y6Th",
        "public.test_data_bitlB6zH",
        "public.test_data_Rk5O9Vfz",
        "public.test_data_C5FP8O6v",
        "public.test_data_QXXtznhh",
        "public.test_data_wYDA4e8t",
        "public.test_data_sbzLZkaL",
        "public.test_data_rQUfSBqs",
        "public.test_data_ORyUP1xs",
        "public.test_data_Ztgs4d76",
        "public.test_data_dekPKgKd",
        "public.test_data_S9AAZJZ5",
        "public.test_data_ZryKkanb",
        "public.test_data_h2FTn4YL",
        "public.test_data_JR83toJV",
        "public.test_data_nxJ7LZSH",
        "public.test_data_2Wjuvesl",
        "public.test_data_8BOYbHFp",
        "public.test_data_7yDhiLp9",
        "public.test_data_a8pjuH8O",
        "public.test_data_eXhbJ7fI",
        "public.test_data_9dEqvs4J",
        "public.test_data_wKokXMiT",
        "public.test_data_nvxxu4If",
        "public.test_data_eqYNFalr",
        "public.test_data_c9NJg68L",
        "public.test_data_LKShy1YM",
        "public.test_data_tK37lGVB",
        "public.test_data_rXkQwjUq",
        "public.test_data_zzlyJipb",
        "public.test_data_k8Mb4eXp",
        "public.test_data_jeD28Yvt",
        "public.test_data_q3O4zEfq",
        "public.test_data_cvuRYzAg",
        "public.test_data_Hx0SGlQK",
        "public.test_data_RGKsDfpT",
        "public.test_data_jfyP01hr",
        "public.test_data_nMwVbcu0",
        "public.test_data_0gWHvHRA",
        "public.test_data_dPOAnIKv",
        "public.test_data_nIyFRek0",
        "public.test_data_RNBP6F3q",
        "public.test_data_1vjAotMO",
        "public.test_data_5LpfVf7X",
        "public.test_data_WKTpLVRJ",
        "public.test_data_gCVBw5Hx",
        "public.test_data_cclSEWcw",
        "public.test_data_yy672vdn",
        "public.test_data_DfbtRZNF",
        "public.expect_table_row_count_to_equal_other_table_data_1",
        "public.expect_table_row_count_to_equal_other_table_data_2",
        "public.expect_table_row_count_to_equal_other_table_data_3",
        "public.test_data_7n9Rsybn",
        "public.test_data_I8KDFhb9",
        "public.test_data_xRW7CgDw",
        "public.test_data_kJIukEJ6",
        "public.test_data_mmkjEOjf",
        "public.test_data_UZuzxnpx",
        "public.test_data_tKUdiY4F",
        "public.test_data_Mz3zWVjN",
        "public.test_data_zklAcvpf",
        "public.test_data_WR1UhXim",
        "public.test_data_AqJAGgme",
        "public.test_data_RHd1bveu",
        "public.test_data_4CbnUFSd",
        "public.test_data_27FpyfML",
        "public.test_data_tF5mgQIA",
        "public.test_data_Jytif28C",
        "public.test_data_kM1t6bkg",
        "public.test_data_slQdZris",
        "public.test_data_EfNCYXlg",
        "public.test_data_av0ZoCwa",
        "public.test_data_S7HCnqTG",
        "public.test_data_kAPqrpjE",
        "public.test_data_R1CIzHqU",
        "public.test_data_Sp3rJ79v",
        "public.test_data_uzw3H8M9",
        "public.test_data_1BJInpIA",
        "public.test_data_74j6u2It",
        "public.test_data_Z1EVh6hJ",
        "public.test_data_k9vMS1IA",
        "public.test_data_qKDvmKbQ",
        "public.test_data_J3kErEmw",
        "public.test_data_hQEt9dRE",
        "public.test_data_ALHlcF8o",
        "public.test_data_QYRex0CC",
        "public.test_data_kgGN4V1V",
        "public.test_data_zUTz1auH",
        "public.test_data_CxOYsbhV",
        "public.test_data_0EHHqOEF",
        "public.test_data_DCrQPio9",
        "public.test_data_d3wekJQ8",
        "public.test_data_3MMdPdJC",
        "public.test_data_SRumxV1N",
        "public.test_data_iq4hymFH",
        "public.test_data_uOCGxz0K",
        "public.test_data_Ajwz24qt",
        "public.test_data_fD64op6G",
        "public.test_data_kySMN2iL",
        "public.test_data_Ug1A3yoo",
        "public.test_data_B4ZuIEi0",
        "public.test_data_CFrWLFec",
        "public.test_data_WL5UAglS",
        "public.test_data_0qNgoJf4",
        "public.test_data_a0yR0aSB",
        "public.test_data_snNYucVq",
        "public.test_data_i4VQHMna",
        "public.test_data_E3lAll2Q",
        "public.test_data_2Rp2kVqy",
        "testing.prices",
    ]

    batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="my_test_datasource",
                data_connector_name="whole_table",
                data_asset_name="testing.prices",
            )
        )
    )
    assert len(batch_definition_list) == 1


def test_skip_inapplicable_tables(empty_data_context):
    context = empty_data_context
    # This test mirrors the likely path to configure a SimpleSqlalchemyDatasource

    db_file = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )

    my_sql_datasource = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
introspection:
    daily:
        skip_inapplicable_tables: true
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"
"""
    )
    print(json.dumps(my_sql_datasource.get_available_data_asset_names(), indent=4))

    assert my_sql_datasource.get_available_data_asset_names() == {
        "daily": [
            "table_containing_id_spacers_for_D",
            "table_full__I",
            "table_partitioned_by_date_column__A",
            "table_with_fk_reference_from_F",
            "view_by_date_column__A",
            "view_with_fk_reference_from_F",
        ]
    }

    with pytest.raises(ge_exceptions.DatasourceInitializationError):
        # noinspection PyUnusedLocal
        my_sql_datasource = context.test_yaml_config(
            f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
introspection:
    daily:
        skip_inapplicable_tables: false
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: date
            date_format_string: "%Y-%m-%d"
    """
        )
