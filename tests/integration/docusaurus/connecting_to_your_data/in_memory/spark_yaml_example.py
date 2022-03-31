from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

# Set up a basic spark session
spark = ge.core.util.get_or_create_spark_application()

# basic dataframe
data = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]
df = spark.createDataFrame(data)

# NOTE: InMemoryStoreBackendDefaults SHOULD NOT BE USED in normal settings. You
# may experience data loss as it persists nothing. It is used here for testing.
# Please refer to docs to learn how to instantiate your DataContext.
store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = BaseDataContext(project_config=data_context_config)

datasource_yaml = """
name: my_spark_dataframe
class_name: Datasource
execution_engine:
    class_name: SparkDFExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - batch_id
"""

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# Here is a RuntimeBatchRequest using a dataframe
batch_request = RuntimeBatchRequest(
    datasource_name="my_spark_dataframe",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={"batch_id": "default_identifier"},
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
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
assert [ds["name"] for ds in context.list_datasources()] == ["my_spark_dataframe"]
assert set(
    context.get_available_data_asset_names()["my_spark_dataframe"][
        "default_runtime_data_connector_name"
    ]
) == {"<YOUR_MEANGINGFUL_NAME>"}
assert validator.validate().success
