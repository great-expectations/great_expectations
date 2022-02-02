import os

from ruamel import yaml
from sqlalchemy import text
from sqlalchemy.engine import create_engine

import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError

ATHENA_DB_NAME = os.getenv("ATHENA_DB_NAME")
ATHENA_STAGING_S3 = os.getenv("ATHENA_STAGING_S3")


connection_string = f"awsathena+rest://@athena.us-east-1.amazonaws.com/{ATHENA_DB_NAME}?s3_staging_dir={ATHENA_STAGING_S3}"

# create datasource and add athena
context = ge.data_context.DataContext()
datasource_yaml = f"""
name: my_awsathena_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  module_name: great_expectations.execution_engine
  connection_string: {connection_string}
data_connectors:
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    batch_identifiers:
      - default_identifier_name
    module_name: great_expectations.datasource.data_connector
  default_inferred_data_connector_name:
    class_name: InferredAssetSqlDataConnector
    module_name: great_expectations.datasource.data_connector
    include_schema_name: true
"""

context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))


# Test 1: Create Temp Table = False
# Note that if you modify this batch request, you may save the new version as a .json file
#  to pass in later the --batch-request option
batch_request = {
    "datasource_name": "my_awsathena_datasource",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": f"{ATHENA_DB_NAME}.taxitable",
    "limit": 1000,
    "batch_spec_passthrough": {"create_temp_table": False},
}
# Feel free to change the name of your suite here. Renaming this will not remove the other one.
expectation_suite_name = "mr_taxi"
try:
    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    print(
        f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
    )
except DataContextError:
    suite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name,
)
validator.head(n_rows=5, fetch_all=False)
assert validator

# Check that new table has not been created
# Create the SQLAlchemy connection. Note that you need to have pyathena installed for this.
engine = create_engine(connection_string)
athena_connection = engine.connect()

result = athena_connection.execute(text(f"SHOW TABLES in {ATHENA_DB_NAME}")).fetchall()
assert len(result) == 1


# second test with the thing?
batch_request = {
    "datasource_name": "my_awsathena_datasource",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": f"{ATHENA_DB_NAME}.taxitable",
    "limit": 1000,
    "batch_spec_passthrough": {"create_temp_table": True},
}
validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name,
)

validator.head(n_rows=5, fetch_all=False)
assert validator

# Check that new table has not been created
# Create the SQLAlchemy connection. Note that you need to have pyathena installed for this.
engine = create_engine(connection_string)
athena_connection = engine.connect()

# we have created a new table
result = athena_connection.execute(text(f"SHOW TABLES in {ATHENA_DB_NAME};")).fetchall()
assert len(result) == 2

for table in result:
    if table[0] != "taxitable":
        athena_connection.execute(text(f"DROP TABLE `{table[0]}`;"))

result = athena_connection.execute(text(f"SHOW TABLES in {ATHENA_DB_NAME};")).fetchall()
assert len(result) == 1
