# <snippet>
import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

# </snippet>

# <snippet>
context = ge.get_context()
# </snippet>

# YAML <snippet>
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
context.add_datasource(**yaml.safe_load(datasource_yaml))
# </snippet>

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python <snippet>
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
context.add_datasource(**datasource_config)
# </snippet>

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

# CLI
datasource_cli = """
<snippet>
great_expectations datasource new
</snippet>
"""

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]

# <snippet>
context.create_expectation_suite("my_expectation_suite")
# </snippet>

# YAML <snippet>
checkpoint_yaml = """
name: my_missing_keys_checkpoint
config_version: 1
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: taxi_datasource
      data_connector_name: default_runtime_data_connector_name
      data_asset_name: taxi_data
    expectation_suite_name: my_expectation_suite
"""
context.add_checkpoint(**yaml.safe_load(checkpoint_yaml))
# </snippet>

test_yaml = context.test_yaml_config(checkpoint_yaml, return_mode="report_object")

# Python <snippet>
checkpoint_config = {
    "name": "my_missing_keys_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "taxi_datasource",
                "data_connector_name": "default_runtime_data_connector_name",
                "data_asset_name": "taxi_data",
            },
            "expectation_suite_name": "my_expectation_suite",
        }
    ],
}
context.add_checkpoint(**checkpoint_config)
# </snippet>

test_python = context.test_yaml_config(
    yaml.dump(checkpoint_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert context.list_checkpoints() == ["my_missing_keys_checkpoint"]

df = pd.read_csv("./data/yellow_tripdata_sample_2019-01.csv")

# <snippet>
results = context.run_checkpoint(
    checkpoint_name="my_missing_keys_checkpoint",
    batch_request={
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {
            "default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER>"
        },
    },
)
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert results["success"] == True

# YAML <snippet>
checkpoint_yaml = """
name: my_missing_batch_request_checkpoint
config_version: 1
class_name: SimpleCheckpoint
expectation_suite_name: my_expectation_suite
"""
context.add_checkpoint(**yaml.safe_load(checkpoint_yaml))
# </snippet>

test_yaml = context.test_yaml_config(checkpoint_yaml, return_mode="report_object")

# Python <snippet>
checkpoint_config = {
    "name": "my_missing_batch_request_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "expectation_suite_name": "my_expectation_suite",
}
context.add_checkpoint(**checkpoint_config)
# </snippet>

test_python = context.test_yaml_config(
    yaml.dump(checkpoint_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert set(context.list_checkpoints()) == {
    "my_missing_keys_checkpoint",
    "my_missing_batch_request_checkpoint",
}

df_1 = pd.read_csv("./data/yellow_tripdata_sample_2019-01.csv")
df_2 = pd.read_csv("./data/yellow_tripdata_sample_2019-02.csv")

# <snippet>
batch_request_1 = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR MEANINGFUL NAME 1>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df_1},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER 1>"},
)

batch_request_2 = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR MEANINGFUL NAME 2>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df_2},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "<YOUR MEANINGFUL IDENTIFIER 2>"},
)

results = context.run_checkpoint(
    checkpoint_name="my_missing_batch_request_checkpoint",
    validations=[
        {"batch_request": batch_request_1},
        {"batch_request": batch_request_2},
    ],
)
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert results["success"] == True
