import os
import pprint

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource import BaseDatasource
from great_expectations.exceptions import StoreBackendError


# Create a GX Data Context
# Make sure GX_CLOUD_ACCESS_TOKEN and GX_CLOUD_ORGANIZATION_ID
# are set in your environment or config_variables.yml
os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<your_gx_cloud_access_token>"
os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id_from_the_app>"

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

# Set variable for a Data Asset
data_asset_name = None
assert data_asset_name, "Please set data_asset_name."

# Set variable for creating a Checkpoint
checkpoint_name = None
assert checkpoint_name, "Please set checkpoint_name."

# Create Datasource
# For simplicity, this script creates a Datasource with a PandasExecutionEngine and a RuntimeDataConnector
try:
    datasource: BaseDatasource = context.get_datasource(datasource_name=datasource_name)
except ValueError:
    datasource_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
        class_name: SqlAlchemyExecutionEngine
        connection_string: ${{my_sql_connection_string}}
    data_connectors:
        {data_connector_name}:
            class_name: InferredAssetSqlDataConnector
            include_schema_name: True
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

# Add Expectations
expectation_configuration = ExpectationConfiguration(
        **{
            "expectation_type": "expect_table_column_count_to_be_between",
            "meta": {},
            "kwargs": {"max_value": 6, "min_value": 4},
        }
    )
# if GX finds an equivalent Expectation, it will be replaced
expectation_suite.add_expectation(
    expectation_configuration=expectation_configuration
)

context.add_or_update_expectation_suite(expectation_suite=expectation_suite)

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
            },
            # additional validations can be configured hereR
        ],
        "config_version": 1,
        "class_name": "Checkpoint",
    }

    context.add_or_update_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_checkpoint(checkpoint_name)
    checkpoint_id = checkpoint.ge_cloud_id

print(f"\n{20*'='}\nCheckpoint Config\n{20*'='}\n")
pprint.pprint(checkpoint)

# Run the Checkpoint:
result = context.run_checkpoint(ge_cloud_id=checkpoint_id)

print(f"\n{20*'='}\nValidation Result\n{20*'='}\n")
pprint.pprint(result)
