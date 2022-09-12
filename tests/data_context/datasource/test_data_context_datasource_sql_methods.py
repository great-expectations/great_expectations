import json
from typing import List, Union

import pytest

from great_expectations.core.batch import Batch, BatchRequest, IDDict
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)

yaml = YAMLHandler()

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None


def test_get_batch(data_context_with_simple_sql_datasource_for_testing_get_batch):
    context = data_context_with_simple_sql_datasource_for_testing_get_batch

    print(
        json.dumps(
            context.datasources["my_sqlite_db"].get_available_data_asset_names(),
            indent=4,
        )
    )

    # Successful specification using a typed BatchRequest
    with pytest.deprecated_call():
        context.get_batch(
            batch_request=BatchRequest(
                datasource_name="my_sqlite_db",
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query=IDDict(
                    batch_filter_parameters={"date": "2020-01-15"}
                ),
            )
        )

        # Failed specification using an untyped BatchRequest
        with pytest.raises(TypeError):
            context.get_batch(
                batch_request={
                    "datasource_name": "my_sqlite_db",
                    "data_connector_name": "daily",
                    "data_asset_name": "table_partitioned_by_date_column__A",
                    "data_connector_query": {
                        "batch_filter_parameters": {"date": "2020-01-15"}
                    },
                }
            )

        # Failed specification using an incomplete BatchRequest
        with pytest.raises(ValueError):
            context.get_batch(
                batch_request=BatchRequest(
                    datasource_name="my_sqlite_db",
                    data_connector_name="daily",
                    data_asset_name="table_partitioned_by_date_column__A",
                    data_connector_query=IDDict(batch_filter_parameters={}),
                )
            )

        # Failed specification using an incomplete BatchRequest
        with pytest.raises(ValueError):
            context.get_batch(
                batch_request=BatchRequest(
                    datasource_name="my_sqlite_db",
                    data_connector_name="daily",
                    data_asset_name="table_partitioned_by_date_column__A",
                )
            )

        # Failed specification using an incomplete BatchRequest
        with pytest.raises(TypeError):
            context.get_batch(
                batch_request=BatchRequest(
                    datasource_name="my_sqlite_db", data_connector_name="daily"
                )
            )

        # Failed specification using an incomplete BatchRequest
        # with pytest.raises(ValueError):
        with pytest.raises(TypeError):
            context.get_batch(
                batch_request=BatchRequest(
                    data_connector_name="daily",
                    data_asset_name="table_partitioned_by_date_column__A",
                    data_connector_query=IDDict(batch_filter_parameters={}),
                )
            )

        # Successful specification using parameters
        context.get_batch(
            datasource_name="my_sqlite_db",
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
            "my_sqlite_db",
            "whole_table",
            "table_partitioned_by_date_column__A",
        )

        # Successful specification using parameters and data_connector_query
        context.get_batch(
            "my_sqlite_db",
            "daily",
            "table_partitioned_by_date_column__A",
            data_connector_query=IDDict(
                {"batch_filter_parameters": {"date": "2020-01-15"}}
            ),
        )

        # Successful specification using parameters and batch_identifiers
        context.get_batch(
            "my_sqlite_db",
            "daily",
            "table_partitioned_by_date_column__A",
            batch_identifiers={"date": "2020-01-15"},
        )


def test_get_validator_bad_batch_request(
    data_context_with_simple_sql_datasource_for_testing_get_batch,
):
    context: "DataContext" = (
        data_context_with_simple_sql_datasource_for_testing_get_batch
    )
    context.create_expectation_suite("my_expectations")
    batch_request: BatchRequest = BatchRequest(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="i_dont_exist",
        data_connector_query=IDDict(batch_filter_parameters={"date": "2020-01-15"}),
    )
    with pytest.raises(KeyError):
        # as a result of introspection, the data_assets will already be loaded into the cache.
        # an incorrect data_asset_name will result in a key error
        context.get_validator(
            batch_request=batch_request, expectation_suite_name="my_expectations"
        )


def test_get_validator(data_context_with_simple_sql_datasource_for_testing_get_batch):
    context = data_context_with_simple_sql_datasource_for_testing_get_batch
    context.create_expectation_suite("my_expectations")

    # Successful specification using a typed BatchRequest
    context.get_validator(
        batch_request=BatchRequest(
            datasource_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query=IDDict(batch_filter_parameters={"date": "2020-01-15"}),
        ),
        expectation_suite_name="my_expectations",
    )

    # Failed specification using an untyped BatchRequest
    with pytest.raises(TypeError):
        context.get_validator(
            batch_request={
                "datasource_name": "my_sqlite_db",
                "data_connector_name": "daily",
                "data_asset_name": "table_partitioned_by_date_column__A",
                "data_connector_query": {
                    "batch_filter_parameters": {"date": "2020-01-15"}
                },
            },
            expectation_suite_name="my_expectations",
        )

    # A BatchRequest specified without the date batch_filter_parameter will return all 30 batches.
    assert (
        len(
            context.get_validator(
                batch_request=BatchRequest(
                    datasource_name="my_sqlite_db",
                    data_connector_name="daily",
                    data_asset_name="table_partitioned_by_date_column__A",
                    data_connector_query=IDDict(batch_filter_parameters={}),
                ),
                expectation_suite_name="my_expectations",
            ).batches
        )
        == 34
    )

    # A BatchRequest specified without a data_connector_query will return all 30 batches.
    assert (
        len(
            context.get_validator(
                batch_request=BatchRequest(
                    datasource_name="my_sqlite_db",
                    data_connector_name="daily",
                    data_asset_name="table_partitioned_by_date_column__A",
                ),
                expectation_suite_name="my_expectations",
            ).batches
        )
        == 34
    )

    # Failed specification using an incomplete BatchRequest
    with pytest.raises(TypeError):
        context.get_validator(
            batch_request=BatchRequest(
                datasource_name="my_sqlite_db", data_connector_name="daily"
            ),
            expectation_suite_name="my_expectations",
        )

    # Failed specification using an incomplete BatchRequest
    # with pytest.raises(ValueError):
    with pytest.raises(TypeError):
        context.get_validator(
            batch_request=BatchRequest(
                data_connector_name="daily",
                data_asset_name="table_partitioned_by_date_column__A",
                data_connector_query=IDDict(batch_filter_parameters={}),
            ),
            expectation_suite_name="my_expectations",
        )

    # Successful specification using parameters
    context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        expectation_suite_name="my_expectations",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # This is the thinnest this can plausibly get.
    context.get_validator(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        expectation_suite_name="my_expectations",
        date="2020-01-15",
    )

    # Successful specification using parameters without parameter names for the identifying triple
    # In the case of a data_asset containing a single Batch, we don't even need parameters
    context.get_validator(
        "my_sqlite_db",
        "whole_table",
        "table_partitioned_by_date_column__A",
        expectation_suite_name="my_expectations",
    )

    # Successful specification using parameters and data_connector_query
    context.get_validator(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        data_connector_query=IDDict(
            {"batch_filter_parameters": {"date": "2020-01-15"}}
        ),
        expectation_suite_name="my_expectations",
    )

    # Successful specification using parameters and batch_identifiers
    context.get_validator(
        "my_sqlite_db",
        "daily",
        "table_partitioned_by_date_column__A",
        batch_identifiers={"date": "2020-01-15"},
        expectation_suite_name="my_expectations",
    )


def test_get_validator_expectation_suite_options(
    data_context_with_simple_sql_datasource_for_testing_get_batch,
):
    context = data_context_with_simple_sql_datasource_for_testing_get_batch
    context.create_expectation_suite("some_expectations")

    # Successful specification with an existing expectation_suite_name
    context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        expectation_suite_name="some_expectations",
        date="2020-01-15",
    )

    # Successful specification with a fetched ExpectationSuite object
    some_expectations = context.get_expectation_suite("some_expectations")
    context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        expectation_suite=some_expectations,
        date="2020-01-15",
    )

    # Successful specification with a fresh ExpectationSuite object
    some_more_expectations = context.create_expectation_suite(
        expectation_suite_name="some_more_expectations"
    )
    context.get_validator(
        datasource_name="my_sqlite_db",
        data_connector_name="daily",
        data_asset_name="table_partitioned_by_date_column__A",
        expectation_suite=some_more_expectations,
        date="2020-01-15",
    )

    # Successful specification using overwrite_existing_expectation_suite
    context.get_validator(
        batch_request=BatchRequest(
            datasource_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            data_connector_query=IDDict(batch_filter_parameters={"date": "2020-01-15"}),
        ),
        create_expectation_suite_with_name="yet_more_expectations",
    )

    # Failed specification: incorrectly typed expectation suite
    with pytest.raises(TypeError):
        context.get_validator(
            datasource_name="my_sqlite_db",
            data_connector_name="daily",
            data_asset_name="table_partitioned_by_date_column__A",
            expectation_suite={
                "im": "a",
                "dictionary": "not a",
                "ExepctationSuite": False,
            },
            date="2020-01-15",
        )


def test_get_batch_list_from_new_style_datasource_with_sql_datasource(
    sa, data_context_with_simple_sql_datasource_for_testing_get_batch
):
    context = data_context_with_simple_sql_datasource_for_testing_get_batch

    batch_request: Union[dict, BatchRequest] = {
        "datasource_name": "my_sqlite_db",
        "data_connector_name": "daily",
        "data_asset_name": "table_partitioned_by_date_column__A",
        "data_connector_query": {"batch_filter_parameters": {"date": "2020-01-15"}},
    }
    batch_list: List[Batch] = context.get_batch_list(**batch_request)

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]
    assert batch.batch_spec is not None
    assert (
        batch.batch_definition["data_asset_name"]
        == "table_partitioned_by_date_column__A"
    )
    assert batch.batch_definition["batch_identifiers"] == {"date": "2020-01-15"}
    assert isinstance(batch.data, SqlAlchemyBatchData)


def test_postgres_datasource_new(sa, empty_data_context):
    context = empty_data_context

    datasource_name = "my_datasource"
    host = "localhost"
    port = "5432"
    username = "postgres"
    password = ""
    database = "test_ci"
    schema_name = "public"
    # A table that you would like to add initially as a Data Asset
    table_name = "yellow_tripdata_sample_2019"

    example_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      credentials:
        host: {host}
        port: '{port}'
        username: {username}
        password: {password}
        database: {database}
        drivername: postgresql
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: true
        introspection_directives:
          schema_name: {schema_name}
      default_configured_data_connector_name:
        class_name: ConfiguredAssetSqlDataConnector
        assets:
          {schema_name}.{table_name}:
            module_name: great_expectations.datasource.data_connector.asset
            class_name: Asset
            include_schema_name: true
    """
    print(example_yaml)
    context.test_yaml_config(yaml_config=example_yaml)


def test_mysql_datasource_new(sa, empty_data_context):
    context = empty_data_context

    datasource_name = "my_datasource"
    host = "localhost"
    port = "3306"
    username = "root"
    password = ""
    database = "test_ci"
    schema_name = "test_ci"
    # A table that you would like to add initially as a Data Asset
    table_name = "yellow_tripdata_sample_2020"

    example_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      credentials:
        host: {host}
        port: '{port}'
        username: {username}
        password: {password}
        database: {database}
        drivername: mysql+pymysql
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: true
        introspection_directives:
          schema_name: {schema_name}
      default_configured_data_connector_name:
        class_name: ConfiguredAssetSqlDataConnector
        assets:
          {schema_name}.{table_name}:
            module_name: great_expectations.datasource.data_connector.asset
            class_name: Asset
            include_schema_name: true
    """
    print(example_yaml)
    context.test_yaml_config(yaml_config=example_yaml)


def test_snowflake_datasource_new(sa, empty_data_context):
    context = empty_data_context
    datasource_name = "my_datasource"

    host = (
        "oca29081.us-east-1"  # The account name (include region -- ex 'ABCD.us-east-1')
    )
    username = "azure_devops"
    database = "DEMO_DB"  # The database name
    schema_name = "TEST_SCHEMA"  # The schema name
    warehouse = "COMPUTE_WH"  # The warehouse name
    role = "PUBLIC"  # The role name
    table_name = (
        "TAXI_DATA_COPY"  # A table that you would like to add initially as a Data Asset
    )
    password = "8BH2uqUx8RZbfKgP"

    example_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      credentials:
        host: {host}
        username: {username}
        database: {database}
        query:
          schema: {schema_name}
          warehouse: {warehouse}
          role: {role}
        password: {password}
        drivername: snowflake
    data_connectors:
      default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: True
        introspection_directives:
          schema_name: {schema_name}
      default_configured_data_connector_name:
        class_name: ConfiguredAssetSqlDataConnector
        module_name: great_expectations.datasource.data_connector
        assets:
          {schema_name}.{table_name}:
            include_schema_name: True
            module_name: great_expectations.datasource.data_connector.asset
            class_name: Asset
            table_name: {table_name}
            schema_name: {schema_name}
    """
    # this is a bug : inferred
    # this is a big : configured

    context.test_yaml_config(yaml_config=example_yaml)
    # add_datasource only if it doesn't already exist in our configuration
    # context.add_datasource(**yaml.load(example_yaml))
    # single_batch_batch_request: BatchRequest = BatchRequest(
    # datasource_name="my_datasource",
    # data_connector_name="default_configured_data_connector_name",
    # data_asset_name="TEST_SCHEMA.TAXI_DATA_COPY",
    # )
    # batch_list = context.get_batch_list(batch_request=single_batch_batch_request)


def test_bigquery_datasource_new(sa, empty_data_context):
    context = empty_data_context
    datasource_name = "my_datasource"
    connection_string = ""
    example_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: {connection_string}
    data_connectors:
      default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
        include_schema_name: True
        introspection_directives:
          schema_name: {schema_name}
      default_configured_data_connector_name:
        class_name: ConfiguredAssetSqlDataConnector
        module_name: great_expectations.datasource.data_connector
        assets:
          {schema_name}.{table_name}:
            include_schema_name: True
            module_name: great_expectations.datasource.data_connector.asset
            class_name: Asset
    """
    # this is a bug : inferred
    # this is a big : configured

    context.test_yaml_config(yaml_config=example_yaml)
    # add_datasource only if it doesn't already exist in our configuration
    # context.add_datasource(**yaml.load(example_yaml))
    # single_batch_batch_request: BatchRequest = BatchRequest(
    # datasource_name="my_datasource",
    # data_connector_name="default_configured_data_connector_name",
    # data_asset_name="TEST_SCHEMA.TAXI_DATA_COPY",
    # )
    # batch_list = context.get_batch_list(batch_request=single_batch_batch_request)
