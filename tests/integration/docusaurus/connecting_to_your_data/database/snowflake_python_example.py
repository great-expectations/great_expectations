import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

sfAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
sfUser = os.environ.get("SNOWFLAKE_USER")
sfPswd = os.environ.get("SNOWFLAKE_PW")
sfDatabase = os.environ.get("SNOWFLAKE_DATABASE")
sfSchema = os.environ.get("SNOWFLAKE_SCHEMA")
sfWarehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

CONNECTION_STRING = f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/{sfDatabase}/{sfSchema}?warehouse={sfWarehouse}"

context = ge.get_context()

datasource_config = {
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
datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# First test for RuntimeBatchRequest using a query
batch_request = RuntimeBatchRequest(
    datasource_name="my_snowflake_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={
        "query": f"SELECT * from {sfSchema.lower()}.taxi_data LIMIT 10"
    },
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
    datasource_name="my_snowflake_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=f"{sfSchema.lower()}.taxi_data",  # this is the name of the table you want to retrieve
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
assert [ds["name"] for ds in context.list_datasources()] == ["my_snowflake_datasource"]
assert f"{sfSchema.lower()}.taxi_data" in set(
    context.get_available_data_asset_names()["my_snowflake_datasource"][
        "default_inferred_data_connector_name"
    ]
)
validator.execution_engine.close()
