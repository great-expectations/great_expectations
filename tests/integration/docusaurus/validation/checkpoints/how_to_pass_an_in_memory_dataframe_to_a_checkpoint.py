# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py imports">
import pandas as pd
from ruamel import yaml

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py get_context">
context = gx.get_context()
# </snippet>

# YAML <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_yaml">
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

# Python <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_config">
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
<snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py datasource_new_cli">
great_expectations datasource new
</snippet>
"""

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py add_expectation_suite">
context.add_or_update_expectation_suite("my_expectation_suite")
# </snippet>

# YAML <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_yaml_missing_keys">
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
context.add_or_update_checkpoint(**yaml.safe_load(checkpoint_yaml))
# </snippet>

test_yaml = context.test_yaml_config(checkpoint_yaml, return_mode="report_object")

# Python <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_python_missing_keys">
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
context.add_or_update_checkpoint(**checkpoint_config)
# </snippet>

test_python = context.test_yaml_config(
    yaml.dump(checkpoint_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert context.list_checkpoints() == ["my_missing_keys_checkpoint"]

df = pd.read_csv("./data/yellow_tripdata_sample_2019-01.csv")

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py run_checkpoint">
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

# YAML <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_yaml_missing_batch_request">
checkpoint_yaml = """
name: my_missing_batch_request_checkpoint
config_version: 1
class_name: SimpleCheckpoint
expectation_suite_name: my_expectation_suite
"""
context.add_or_update_checkpoint(**yaml.safe_load(checkpoint_yaml))
# </snippet>

test_yaml = context.test_yaml_config(checkpoint_yaml, return_mode="report_object")

# Python <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py checkpoint_config_python_missing_batch_request">
checkpoint_config = {
    "name": "my_missing_batch_request_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "expectation_suite_name": "my_expectation_suite",
}
context.add_or_update_checkpoint(**checkpoint_config)
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

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py runtime_batch_request">
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
