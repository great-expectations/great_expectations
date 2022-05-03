import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

redshift_username = os.environ.get("REDSHIFT_USERNAME")
redshift_password = os.environ.get("REDSHIFT_PASSWORD")
redshift_host = os.environ.get("REDSHIFT_HOST")
redshift_port = os.environ.get("REDSHIFT_PORT")
redshift_database = os.environ.get("REDSHIFT_DATABASE")
redshift_sslmode = os.environ.get("REDSHIFT_SSLMODE")

sfAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
sfUser = os.environ.get("SNOWFLAKE_USER")
sfPswd = os.environ.get("SNOWFLAKE_PW")
sfDatabase = os.environ.get("SNOWFLAKE_DATABASE")
sfSchema = os.environ.get("SNOWFLAKE_SCHEMA")
sfWarehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

SF_CONNECTION_STRING = f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/{sfDatabase}/{sfSchema}?warehouse={sfWarehouse}"

CONNECTION_STRING = f"postgresql+psycopg2://{redshift_username}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_database}?sslmode={redshift_sslmode}"

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

load_data_into_test_database(
    table_name="taxi_data",
    csv_path="tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    connection_string=CONNECTION_STRING,
)

load_data_into_test_database(
    table_name="taxi_data",
    csv_path="tests/test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    connection_string=SF_CONNECTION_STRING,
)

context = ge.get_context()

datasource_config = {
    "name": "my_redshift_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>",
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

sf_datasource_config = {
    "name": "my_snowflake_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>",
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

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
sf_datasource_config["execution_engine"]["connection_string"] = SF_CONNECTION_STRING

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

batch_request = BatchRequest(
    datasource_name="my_redshift_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="default_name",
)

expectation_suite_name = "compare_two_tables"

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=expectation_suite_name
)
