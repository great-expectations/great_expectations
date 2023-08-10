from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py import gx">
import great_expectations as gx

# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py import BatchRequest">
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py get_context">
context = gx.get_context()
# </snippet>

datasource_yaml = f"""
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: <PATH_TO_YOUR_DATA_HERE>
        default_regex:
          group_names:
            - data_asset_name
          pattern: (.*)
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace("<PATH_TO_YOUR_DATA_HERE>", "../data/")

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = "./data/yellow_tripdata_sample_2019-01.csv"

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019-01.csv"

# Example using batch request to get a batch:
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py context.get_batch with batch request">
batch = context.get_batch(batch_request=batch_request)
# </snippet>

# Example using parameters to get a batch:
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py context.get_batch with parameters data_asset_name">
data_asset_name = "<YOUR_DATA_ASSET_NAME>"
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the get_batch call below
data_asset_name = "yellow_tripdata_sample_2019-01.csv"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py context.get_batch with parameters">
context.get_batch(
    datasource_name="taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=data_asset_name,
)
# </snippet>

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_tripdata_sample_2019-01.csv" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)
