import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

# YAML
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

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python
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

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

assert test_yaml == test_python

context.add_datasource(**datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR MEANINGFUL NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH TO YOUR DATA HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER>"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters[
    "path"
] = "./data/single_directory_one_data_asset/yellow_tripdata_2019-01.csv"

validator = context.get_validator(
    batch_request=batch_request,
    create_expectation_suite_with_name="<MY EXPECTATION SUITE NAME>",
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "<YOUR MEANINGFUL NAME>" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_runtime_data_connector_name"
    ]
)

path = "<PATH TO YOUR DATA HERE>"
# Please note this override is only to provide good UX for docs and tests.
path = "./data/single_directory_one_data_asset/yellow_tripdata_2019-01.csv"
df = pd.read_csv(path)

batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR MEANINGFUL NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER>"},
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="<MY EXPECTATION SUITE NAME>",
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "<YOUR MEANINGFUL NAME>" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_runtime_data_connector_name"
    ]
)
