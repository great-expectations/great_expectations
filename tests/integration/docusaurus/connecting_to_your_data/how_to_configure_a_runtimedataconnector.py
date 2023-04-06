# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py import pandas">
import pandas as pd

# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py yaml imports">
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py python imports">
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()
# </snippet>
# </snippet>

# YAML
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py datasource_config yaml">
datasource_yaml = r"""
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
"""
# </snippet>

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py python datasource_config">
datasource_config = {
    "name": "taxi_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}
# </snippet>

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

assert test_yaml == test_python

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py add_datasource">
context.add_datasource(**datasource_config)
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py batch_request 1">
batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="YOUR_MEANINGFUL_NAME",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"path": "PATH_TO_YOUR_DATA_HERE"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "YOUR_MEANINGFUL_IDENTIFIER"},
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters[
    "path"
] = "./data/single_directory_one_data_asset/yellow_tripdata_2019-01.csv"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py get_validator 1">
validator = context.get_validator(
    batch_request=batch_request,
    create_expectation_suite_with_name="MY_EXPECTATION_SUITE_NAME",
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "YOUR_MEANINGFUL_NAME" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_runtime_data_connector_name"
    ]
)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py path">
path = "PATH_TO_YOUR_DATA_HERE"
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
path = "./data/single_directory_one_data_asset/yellow_tripdata_2019-01.csv"
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py batch_request example 2">
df = pd.read_csv(path)

batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="YOUR_MEANINGFUL_NAME",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "YOUR_MEANINGFUL_IDENTIFIER"},
)
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_runtimedataconnector.py get_validator example 2">
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="MY_EXPECTATION_SUITE_NAME",
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "YOUR_MEANINGFUL_NAME" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_runtime_data_connector_name"
    ]
)
