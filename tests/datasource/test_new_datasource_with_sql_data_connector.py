import json
import os
import random

import pytest
from ruamel.yaml import YAML

try:
    import pandas as pd
except ImportError:
    pd = None

    logger.debug(
        "Unable to load pandas; install optional pandas dependency for support."
    )

import great_expectations.exceptions as ge_exceptions
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.validator.validator import Validator

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

try:
    import sqlalchemy_bigquery as sqla_bigquery
except ImportError:
    try:
        import pybigquery.sqlalchemy_bigquery as sqla_bigquery
    except ImportError:
        sqla_bigquery = None

yaml = YAML()


@pytest.fixture
def data_context_with_sql_data_connectors_including_schema_for_testing_get_batch(
    sa,
    empty_data_context,
    test_db_connection_string,
):
    context: DataContext = empty_data_context

    sqlite_engine: sa.engine.base.Engine = sa.create_engine(test_db_connection_string)
    # noinspection PyUnusedLocal
    conn: sa.engine.base.Connection = sqlite_engine.connect()
    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: SqlAlchemyExecutionEngine
            connection_string: {test_db_connection_string}

        data_connectors:
            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
            my_inferred_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: InferredAssetSqlDataConnector
                include_schema_name: true
            my_configured_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: ConfiguredAssetSqlDataConnector
                assets:
                    my_first_data_asset:
                        table_name: table_1
                    my_second_data_asset:
                        schema_name: main
                        table_name: table_2
                    table_1: {{}}
                    table_2:
                        schema_name: main
    """

    try:
        # noinspection PyUnusedLocal
        my_sql_datasource: Optional[
            Union[SimpleSqlalchemyDatasource, LegacyDatasource]
        ] = context.add_datasource(
            "test_sqlite_db_datasource", **yaml.load(datasource_config)
        )
    except AttributeError:
        pytest.skip("SQL Database tests require sqlalchemy to be installed.")

    return context


def test_basic_instantiation_with_ConfiguredAssetSqlDataConnector(sa):
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

        assets:
            table_partitioned_by_date_column__A:
                splitter_method: _split_on_converted_datetime
                splitter_kwargs:
                    column_name: date
                    date_format_string: "%Y-%W"
    """,
    )

    my_data_source = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_sql_datasource"},
    )

    report = my_data_source.self_check()
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
                # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
                # "example_data_reference": {
                #     "batch_spec": {
                #         "table_name": "table_partitioned_by_date_column__A",
                #         "data_asset_name": "table_partitioned_by_date_column__A",
                #         "batch_identifiers": {"date": "2020-01"},
                #         "splitter_method": "_split_on_converted_datetime",
                #         "splitter_kwargs": {
                #             "column_name": "date",
                #             "date_format_string": "%Y-%W",
                #         },
                #     },
                #     "n_rows": 24,
                # },
            },
        },
    }


def test_basic_instantiation_with_InferredAssetSqlDataConnector(sa):
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
        class_name: InferredAssetSqlDataConnector
        name: whole_table
        data_asset_name_prefix: prefix__
        data_asset_name_suffix: __xiffus
    """,
    )

    my_data_source = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource"},
        runtime_environment={"name": "my_sql_datasource"},
    )
    report = my_data_source.self_check()

    connection_string_to_test = f"""sqlite:///{db_file}"""
    assert report == {
        "execution_engine": {
            "connection_string": connection_string_to_test,
            "module_name": "great_expectations.execution_engine.sqlalchemy_execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
        },
        "data_connectors": {
            "count": 1,
            "my_sqlite_db": {
                "class_name": "InferredAssetSqlDataConnector",
                "data_asset_count": 21,
                "example_data_asset_names": [
                    "prefix__table_containing_id_spacers_for_D__xiffus",
                    "prefix__table_full__I__xiffus",
                    "prefix__table_partitioned_by_date_column__A__xiffus",
                ],
                "data_assets": {
                    "prefix__table_containing_id_spacers_for_D__xiffus": {
                        "batch_definition_count": 1,
                        "example_data_references": [{}],
                    },
                    "prefix__table_full__I__xiffus": {
                        "batch_definition_count": 1,
                        "example_data_references": [{}],
                    },
                    "prefix__table_partitioned_by_date_column__A__xiffus": {
                        "batch_definition_count": 1,
                        "example_data_references": [{}],
                    },
                },
                "unmatched_data_reference_count": 0,
                "example_unmatched_data_references": [],
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
    datasource_with_minimum_config = context.test_yaml_config(
        f"""
class_name: SimpleSqlalchemyDatasource
connection_string: sqlite:///{db_file}
"""
        + """
introspection:
    whole_table: {}
"""
    )
    print(
        json.dumps(
            datasource_with_minimum_config.get_available_data_asset_names(), indent=4
        )
    )

    assert datasource_with_minimum_config.get_available_data_asset_names() == {
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

    assert datasource_with_minimum_config.get_available_data_asset_names_and_types() == {
        "whole_table": [
            ("table_containing_id_spacers_for_D", "table"),
            ("table_full__I", "table"),
            ("table_partitioned_by_date_column__A", "table"),
            ("table_partitioned_by_foreign_key__F", "table"),
            ("table_partitioned_by_incrementing_batch_id__E", "table"),
            (
                "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "table",
            ),
            ("table_partitioned_by_multiple_columns__G", "table"),
            (
                "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
                "table",
            ),
            ("table_partitioned_by_timestamp_column__B", "table"),
            ("table_that_should_be_partitioned_by_random_hash__H", "table"),
            ("table_with_fk_reference_from_F", "table"),
            ("view_by_date_column__A", "view"),
            ("view_by_incrementing_batch_id__E", "view"),
            (
                "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "view",
            ),
            ("view_by_multiple_columns__G", "view"),
            ("view_by_regularly_spaced_incrementing_id_column__C", "view"),
            ("view_by_timestamp_column__B", "view"),
            ("view_containing_id_spacers_for_D", "view"),
            ("view_partitioned_by_foreign_key__F", "view"),
            ("view_that_should_be_partitioned_by_random_hash__H", "view"),
            ("view_with_fk_reference_from_F", "view"),
        ]
    }

    # Here we should test getting a batch

    # Very thin starting config
    datasource_with_name_suffix = context.test_yaml_config(
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

    assert datasource_with_name_suffix.get_available_data_asset_names() == {
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

    assert datasource_with_name_suffix.get_available_data_asset_names_and_types() == {
        "whole_table": [
            ("table_containing_id_spacers_for_D", "table"),
            ("table_full__I", "table"),
            ("table_partitioned_by_date_column__A", "table"),
            ("table_partitioned_by_foreign_key__F", "table"),
            ("table_partitioned_by_incrementing_batch_id__E", "table"),
            (
                "table_partitioned_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "table",
            ),
            ("table_partitioned_by_multiple_columns__G", "table"),
            (
                "table_partitioned_by_regularly_spaced_incrementing_id_column__C",
                "table",
            ),
            ("table_partitioned_by_timestamp_column__B", "table"),
            ("table_that_should_be_partitioned_by_random_hash__H", "table"),
            ("table_with_fk_reference_from_F", "table"),
            ("view_by_date_column__A", "view"),
            ("view_by_incrementing_batch_id__E", "view"),
            (
                "view_by_irregularly_spaced_incrementing_id_with_spacing_in_a_second_table__D",
                "view",
            ),
            ("view_by_multiple_columns__G", "view"),
            ("view_by_regularly_spaced_incrementing_id_column__C", "view"),
            ("view_by_timestamp_column__B", "view"),
            ("view_containing_id_spacers_for_D", "view"),
            ("view_partitioned_by_foreign_key__F", "view"),
            ("view_that_should_be_partitioned_by_random_hash__H", "view"),
            ("view_with_fk_reference_from_F", "view"),
        ]
    }

    # Here we should test getting a batch

    # Add some manually configured tables...
    datasource_manually_configured = context.test_yaml_config(
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

    print(
        json.dumps(
            datasource_manually_configured.get_available_data_asset_names(), indent=4
        )
    )
    assert datasource_manually_configured.get_available_data_asset_names() == {
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

    # can't use get_available_data_asset_names_and_types here because it's only implemented
    # on InferredAssetSqlDataConnector, not ConfiguredAssetSqlDataConnector
    with pytest.raises(NotImplementedError):
        datasource_manually_configured.get_available_data_asset_names_and_types()

    # Here we should test getting another batch

    # Drop the introspection...
    datasource_without_introspection = context.test_yaml_config(
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
    print(
        json.dumps(
            datasource_without_introspection.get_available_data_asset_names(), indent=4
        )
    )
    assert datasource_without_introspection.get_available_data_asset_names() == {
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


@pytest.mark.skipif(
    sqla_bigquery is None,
    reason="sqlalchemy_bigquery/pybigquery is not installed",
)
def test_basic_instantiation_with_bigquery_creds(sa, empty_data_context):
    context = empty_data_context
    my_data_source = instantiate_class_from_config(
        # private key is valid but useless
        config={
            "connection_string": "bigquery://project-1353/dataset",
            "credentials_info": {
                "type": "service_account",
                "project_id": "project-1353",
                "private_key_id": "df87033061fd7c27dcc953e235fe099a7017f9c4",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDJGtBAt//4Mro3\n58HfA8JfoX1KtTtCCsvGqVhXY5Z74q7knLJUm3cy6pheIUrJ7qvTSuzQZrguI+x5\n/J3+iosjgEhtkHfkDwPreW/K5TL/JXh1xjr/qMro4P10csVmmiAtVDypFIj2Tl4f\n6oJ5qGJJ6WBtFmswgHZeSZCis3FYIpho9Dqzj/Uahji4/ApaY4s/O463ymr4VhFW\n3ieIiepUB/XBBYzJOa5xAmo5HA6aSw7SJX+HUdxqHyhdsIz6kNV+3ej/YF4hgzVF\n2vn2+/+71GqSaZKK12frdfFU4VkWMzgvr2tO9XYX49tPOHTlZdEmi1UV2VuS2M39\nsuHHpaOTAgMBAAECggEARRGg+cFYN/nQLDg8RSiI3whbPEffSNDlaN8rmKP7AKR7\ntce9lcJpX4Lj/txHT/BZcjG3AOJummY7JzBkYRJbND+wYHTwQFMJ4Rttkk1CxQ+s\n/iItjDYALphrZE2wz4rax0a5qMaFPbbvq92Cn17+Fu2A8SZ0fQ152etBMigYIxDu\nzvmT4Fb67zJF5BOt8ay75a50H2sxtJcOWiaFX4Esil7+9gJft04MHSsZXBh8GgQK\nKo8xFlBe37tT1vE6Np0igxJIm+HjqObOpQ0pkaE1H2/rWt7k+HfuEWxKYva2t8C2\neexSupeTj1AkwgzKhk7sbyKMieTWg8+Tc3UODxZEMQKBgQDm4Slc8g5e59xw/Wg2\neyM/Gv2Syh1NLDChuFph4A0SBZhNYG7v5BZE4hVIHfZrrLzqSq7KQ2RojDIQzB7u\nUnrtdblvjZcwc1eepgbB9yjcUifaANZg8k9ukN2V0glPvYtIE1yHX6p8kvrIOlEb\n+WxTWXecmq8FL5QnUvboByFogwKBgQDe/FCneKBmaQPEJWa1cx2izA3JFyViRKx5\nWMv3CqEAL96YuL2JOnEvZFGeG+Bup8nohA8YD58TaED58vKjs9j0WFxw9T2V5d9M\n24fT0AwcZMnnrMKslC6ShyWMzntqHL3FvLh4jBthcJ+fsJbLN+vs/7qhU0qWht26\nvCzqAPpLsQKBgAmmMWdcCnO29wSV4qwcO13gz+Y5oj3ece3gWY6roYA2UaYyOJC4\nFKIuXLtV3T2ky4RzOJjldiXUbic7kLNfKRoRiH18CmyQ9YGA6NlkbgW/PUEkNdF5\nbw5s6YXgcFkvz8lkXcKeoe5w6iBCJ6+mnfthytj1sgjicyutkPojiibnAoGAKBEu\nbOk/6Sb1hkkyK2iD6ry/vWJrVT0BwMwz2jAOvfncBZUseXwG2n0sLTzVFw0POrh/\n/dLQwqv5APCmqMOoOD+oXKO0bTrg5O6NeYHoqzFxFi/0yw3VUH74QFTZ2DdR4jYG\n76I9SUTzab5RWjKyMePBpTtSK7oQHX2ylFmYoAECgYBmkwKQkj1b0WZ3+lbb79Jc\nZayAGUgg/W1Fh0V1m8on8wzGOoBoYSbmriyUlUycEivnJaskxCU1Yac2hNQS8KaU\nnm/gd3D0/8ghNW9szvvvRKc99dpgU6OYHvESq4+vG5gdIDHm2C5jMQGQgf1l2VOV\n0z7hg6e3jgecJweN7Yzfnw==\n-----END PRIVATE KEY-----\n",
                "client_email": "testme@project-1353.iam.gserviceaccount.com",
                "client_id": "100945395817716260007",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40project-1353.iam.gserviceaccount.com",
            },
        },
        config_defaults={
            "module_name": "great_expectations.datasource",
            "class_name": "SimpleSqlalchemyDatasource",
        },
        runtime_environment={"name": "my_sql_datasource"},
    )

    # bigquery driver is invoked upon datasource instantiation, and validates credentials_info
    print(my_data_source)


def test_basic_instantiation_with_bigquery_creds_failure_pkey(sa, empty_data_context):
    context = empty_data_context
    try:
        my_data_source = instantiate_class_from_config(
            # private key is valid but useless
            config={
                "connection_string": "bigquery://project-1353/dataset",
                "credentials_info": {
                    "type": "service_account",
                    "project_id": "project-1353",
                    "private_key_id": "df87033061fd7c27dcc953e235fe099a7017f9c4",
                    "private_key": "bad_pkey",
                    "client_email": "testme@project-1353.iam.gserviceaccount.com",
                    "client_id": "100945395817716260007",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40project-1353.iam.gserviceaccount.com",
                },
            },
            config_defaults={
                "module_name": "great_expectations.datasource",
                "class_name": "SimpleSqlalchemyDatasource",
            },
            runtime_environment={"name": "my_sql_datasource"},
        )
    except:
        return

    raise Exception("BigQuery incorrectly passed with invalid private key")

    print(my_data_source)


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


def test_batch_request_sql_with_schema(
    data_context_with_sql_data_connectors_including_schema_for_testing_get_batch,
):
    context: DataContext = (
        data_context_with_sql_data_connectors_including_schema_for_testing_get_batch
    )

    df_table_expected_my_first_data_asset: pd.DataFrame = pd.DataFrame(
        {"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]}
    )
    df_table_expected_my_second_data_asset: pd.DataFrame = pd.DataFrame(
        {"col_1": [0, 1, 2, 3, 4], "col_2": ["b", "c", "d", "e", "f"]}
    )

    batch_request: dict
    validator: Validator
    df_table_actual: pd.DataFrame

    # Exercise RuntimeDataConnector using SQL query against database table with empty schema name.
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_runtime_data_connector",
        "data_asset_name": "test_asset",
        "runtime_parameters": {"query": "SELECT * FROM table_1"},
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
        },
    }
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise RuntimeDataConnector using SQL query against database table with non-empty ("main") schema name.
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_runtime_data_connector",
        "data_asset_name": "test_asset",
        "runtime_parameters": {"query": "SELECT * FROM main.table_2"},
        "batch_identifiers": {
            "pipeline_stage_name": "core_processing",
            "airflow_run_id": 1234567890,
        },
    }
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise InferredAssetSqlDataConnector using data_asset_name introspected with schema from table, named "table_1".
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_inferred_data_connector",
        "data_asset_name": "main.table_1",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise InferredAssetSqlDataConnector using data_asset_name introspected with schema from table, named "table_2".
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_inferred_data_connector",
        "data_asset_name": "main.table_2",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "table_1" (implicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "my_first_data_asset",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "table_2" (implicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "main.my_second_data_asset",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "table_1" (explicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "table_1",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_first_data_asset)

    # Exercise ConfiguredAssetSqlDataConnector using data_asset_name corresponding to "table_2" (explicitly).
    batch_request = {
        "datasource_name": "test_sqlite_db_datasource",
        "data_connector_name": "my_configured_data_connector",
        "data_asset_name": "main.table_2",
    }
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite=ExpectationSuite(
            "my_expectation_suite", data_context=context
        ),
    )
    df_table_actual = validator.head(n_rows=0, fetch_all=True).drop(columns=["index"])
    assert df_table_actual.equals(df_table_expected_my_second_data_asset)
