from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING = "trino://test@localhost:8088/memory/schema"

# This utility is not for general use. It is only to support testing.
load_data_into_test_database(
    table_name="taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string=CONNECTION_STRING,
)

# <snippet>
context = ge.get_context()
# </snippet>

# <snippet>
datasource_config = {
    "name": "my_trino_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "trino://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<CATALOG>/<SCHEMA>",
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
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING

# <snippet>
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet>
context.add_datasource(**datasource_config)
# </snippet>

# Here is a RuntimeBatchRequest using a query
batch_request = RuntimeBatchRequest(
    datasource_name="my_trino_datasource",
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

# Here is a BatchRequest naming a table
batch_request = BatchRequest(
    datasource_name="my_trino_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="schema.taxi_data",  # this is the name of the table you want to retrieve
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
assert [ds["name"] for ds in context.list_datasources()] == ["my_trino_datasource"]
assert "schema.taxi_data" in set(
    context.get_available_data_asset_names()["my_trino_datasource"][
        "default_inferred_data_connector_name"
    ]
)
