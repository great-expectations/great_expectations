from ruamel import yaml

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
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
    "module_name": "great_expectations.datasource",  # in shared config
    "class_name": "Datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",  # in shared config
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "trino://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<CATALOG>/<SCHEMA>",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            # "batch_identifiers": ["default_identifier_name"],
            "batch_identifiers": ["query_string"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetSqlDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "assets": {
                "taxi_data": {
                    # "include_schema_name": False,
                    # # "schema_name": "schema2",
                },
            },
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
runtime_batch_request = RuntimeBatchRequest(
    datasource_name="my_trino_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    # batch_identifiers={"default_identifier_name": "default_identifier"},
    batch_identifiers={"query_string": "whatever"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=runtime_batch_request, expectation_suite_name="test_suite"
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

# data_assistant_result = context.assistants.onboarding.run(batch_request=runtime_batch_request)

# Some Configured Asset stuff

# Here is a BatchRequest naming a table
configured_batch_request = BatchRequest(
    datasource_name="my_trino_datasource",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="taxi_data",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=configured_batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# Trying column_max Expectation
validator_result = validator.expect_column_max_to_be_between(
    column="fare_amount", min_value=1, max_value=30
)
print(f"\n\nvalidator_result (column_max_between):\n{validator_result}\n\n")

context.create_expectation_suite(
    expectation_suite_name="onboarding_suite", overwrite_existing=True
)

exclude_column_names = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
]
# Run onboarding assistant with configured
data_assistant_result = context.assistants.onboarding.run(
    # batch_request=configured_batch_request,
    batch_request=runtime_batch_request,
    exclude_column_names=exclude_column_names,
)
# data_assistant_result.metrics_by_domain
print(
    f"\n\ndata_assistant_result.metrics_by_domain :\n{data_assistant_result.metrics_by_domain}\n\n"
)
expectation_suite = data_assistant_result.get_expectation_suite(
    expectation_suite_name="onboarding_suite"
)
context.save_expectation_suite(
    expectation_suite=expectation_suite, discard_failed_expectations=False
)

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": configured_batch_request,
            "expectation_suite_name": "onboarding_suite",
        }
    ],
}
checkpoint = SimpleCheckpoint(
    "yellow_tripdata_sample_onboarding_suite",
    context,
    **checkpoint_config,
)
checkpoint_result = checkpoint.run()
print(f"\n\ncheckpoint_result:\n{checkpoint_result}\n\n")
