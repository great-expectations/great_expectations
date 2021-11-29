import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

# YAML
datasource_yaml = """
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

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python

context.add_datasource(**datasource_config)

# NOTE: The following code is only for testing and can be ignored by users.
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]

# YAML
checkpoint_yaml = """
name: my_checkpoint
config_version: 1
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: taxi_datasource
      data_connector_name: default_runtime_data_connector_name
      data_asset_name: taxi_data
      data_connector_query:
        index: -1
    expectation_suite_name: my_suite
"""

test_yaml = context.test_yaml_config(checkpoint_yaml, return_mode="report_object")

# Python
checkpoint_config = {
    "name": "my_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "taxi_datasource",
                "data_connector_name": "default_runtime_data_connector_name",
                "data_asset_name": "taxi_data",
                "data_connector_query": {"index": -1},
            },
            "expectation_suite_name": "my_suite",
        }
    ],
}

test_python = context.test_yaml_config(
    yaml.dump(checkpoint_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python

context.add_checkpoint(**checkpoint_config)

# NOTE: The following code is only for testing and can be ignored by users.
assert context.list_checkpoints() == ["my_checkpoint"]

path = "./data/yellow_tripdata_sample_2019-01.csv"
df = pd.read_csv(path)

batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR MEANINGFUL NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER>"},
)


