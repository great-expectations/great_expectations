import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.util import get_context

yaml = YAMLHandler()

# Set up a basic spark session
spark = gx.core.util.get_or_create_spark_application()

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
context = get_context(project_config=data_context_config)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py config">
datasource_config = {
    "name": "my_spark_dataframe",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["batch_id"],
        }
    },
}
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py test yaml_config">
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py add datasource">
context.add_datasource(**datasource_config)
# </snippet>

# Here is a RuntimeBatchRequest using a dataframe
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py batch request">
batch_request = RuntimeBatchRequest(
    datasource_name="my_spark_dataframe",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={"batch_id": "default_identifier"},
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py validator">
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_spark_dataframe"]
assert set(
    context.get_available_data_asset_names()["my_spark_dataframe"][
        "default_runtime_data_connector_name"
    ]
) == {"<YOUR_MEANGINGFUL_NAME>"}
assert validator.validate().success
