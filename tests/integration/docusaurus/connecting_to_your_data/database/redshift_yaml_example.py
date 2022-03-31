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

CONNECTION_STRING = f"postgresql+psycopg2://{redshift_username}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_database}?sslmode={redshift_sslmode}"

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

load_data_into_test_database(
    table_name="taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=CONNECTION_STRING,
)

context = ge.get_context()

datasource_yaml = """
name: my_redshift_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       include_schema_name: true
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>",
    CONNECTION_STRING,
)

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# First test for RuntimeBatchRequest using a query
batch_request = RuntimeBatchRequest(
    datasource_name="my_redshift_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)

# Second test for BatchRequest naming a table
batch_request = BatchRequest(
    datasource_name="my_redshift_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="taxi_data",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_redshift_datasource"]
assert "taxi_data" in set(
    context.get_available_data_asset_names()["my_redshift_datasource"][
        "default_inferred_data_connector_name"
    ]
)
