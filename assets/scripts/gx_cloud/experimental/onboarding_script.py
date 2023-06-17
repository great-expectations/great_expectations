import pprint

import pandas as pd

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource import BaseDatasource
from great_expectations.exceptions import StoreBackendError
from great_expectations.validator.validator import Validator

# Create a GX Data Context
# Make sure GX_CLOUD_ACCESS_TOKEN and GX_CLOUD_ORGANIZATION_ID
# are set in your environment or config_variables.yml
context: CloudDataContext = gx.get_context(
    cloud_mode=True,
)

# Set variables for creating a Datasource
datasource_name = None
data_connector_name = (
    "default_runtime_data_connector_name"  # Optional: Set your own data_connector_name
)
assert datasource_name, "Please set datasource_name."

# Set variable for creating an Expectation Suite
expectation_suite_name = None
assert expectation_suite_name, "Please set expectation_suite_name."

# Set variables for connecting a Validator to a Data Asset, along with a Batch of data
data_asset_name = None
assert data_asset_name, "Please set data_asset_name."
path_to_validator_batch = None  # e.g. "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
assert (
    path_to_validator_batch
), "Please set path_to_validator_batch. This can be a local filepath or a remote URL."

# Set variable for creating a Checkpoint
checkpoint_name = None
assert checkpoint_name, "Please set checkpoint_name."

# Set variable to get a Batch of data to validate against the new Checkpoint
path_to_batch_to_validate = None  # e.g. "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
assert (
    path_to_batch_to_validate
), "Please set path_to_batch_to_validate. This can be a local filepath or a remote URL."

# Create Datasource
# For simplicity, this script creates a Datasource with a PandasExecutionEngine and a RuntimeDataConnector
try:
    datasource: BaseDatasource = context.get_datasource(datasource_name=datasource_name)
except ValueError:
    datasource_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        {data_connector_name}:
            class_name: RuntimeDataConnector
            batch_identifiers:
            - path
    """
    # Test your configuration:
    datasource: BaseDatasource = context.test_yaml_config(datasource_yaml)

    # Save your Datasource:
    datasource: BaseDatasource = context.add_or_update_datasource(datasource=datasource)

print(f"\n{20*'='}\nDatasource Config\n{20*'='}\n")
pprint.pprint(datasource.config)

# Create a new Expectation Suite
try:
    expectation_suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    expectation_suite_ge_cloud_id = expectation_suite.ge_cloud_id
except StoreBackendError:
    expectation_suite: ExpectationSuite = context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    expectation_suite_ge_cloud_id = expectation_suite.ge_cloud_id

# Connect a Batch of data to a Validator to add Expectations interactively
batch_df: pd.DataFrame = pd.read_csv(path_to_validator_batch)

batch_request = RuntimeBatchRequest(
    runtime_parameters={"batch_data": batch_df},
    batch_identifiers={"path": path_to_validator_batch},
    datasource_name=datasource_name,
    data_connector_name=data_connector_name,
    data_asset_name=data_asset_name,
)
validator: Validator = context.get_validator(
    expectation_suite_name=expectation_suite_name, batch_request=batch_request
)

# Add Expectations interactively using tab completion
validator.expect_column_to_exist(column="")

# Save Expectation Suite
validator.save_expectation_suite(discard_failed_expectations=False)
expectation_suite: ExpectationSuite = context.get_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
print(f"\n{20*'='}\nExpectation Suite\n{20*'='}\n")
pprint.pprint(expectation_suite)

# Create a new Checkpoint
try:
    checkpoint: Checkpoint = context.get_checkpoint(checkpoint_name)
    checkpoint_id = checkpoint.ge_cloud_id
except StoreBackendError:
    checkpoint_config = {
        "name": checkpoint_name,
        "validations": [
            {
                "expectation_suite_name": expectation_suite_name,
                "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
                "batch_request": {
                    "datasource_name": datasource_name,
                    "data_connector_name": data_connector_name,
                    "data_asset_name": data_asset_name,
                },
            }
        ],
        "config_version": 1,
        "class_name": "Checkpoint",
    }

    context.add_or_update_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_checkpoint(checkpoint_name)
    checkpoint_id = checkpoint.ge_cloud_id

print(f"\n{20*'='}\nCheckpoint Config\n{20*'='}\n")
pprint.pprint(checkpoint)

# Get a Checkpoint snippet to use in a CI script
run_checkpoint_snippet = f"""\
import pprint

import great_expectations as gx
import pandas as pd

path_to_batch_to_validate = None
assert path_to_batch_to_validate is not None, "Please set path_to_batch_to_validate. This can be a local filepath or a remote URL."
validation_df = pd.read_csv(path_to_batch_to_validate)

result = context.run_checkpoint(
    ge_cloud_id="{checkpoint_id}",
    batch_request={{
        "runtime_parameters": {{
            "batch_data": validation_df
        }},
        "batch_identifiers": {{
            "path": path_to_batch_to_validate
        }},
    }}
)
ppint.pprint(result)
"""

print(f"\n{20*'='}\nCheckpoint Snippet\n{20*'='}\n")
print(run_checkpoint_snippet)

# Run the Checkpoint:
validation_df: pd.DataFrame = pd.read_csv(path_to_batch_to_validate)

result = context.run_checkpoint(
    ge_cloud_id=checkpoint_id,
    batch_request={
        "runtime_parameters": {"batch_data": validation_df},
        "batch_identifiers": {"path": path_to_batch_to_validate},
    },
)

print(f"\n{20*'='}\nValidation Result\n{20*'='}\n")
pprint.pprint(result)
