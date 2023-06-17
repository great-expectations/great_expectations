import pprint

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest, Datasource
from great_expectations.datasource.fluent.pandas_datasource import CSVAsset

# Make sure GX_CLOUD_ACCESS_TOKEN and GX_CLOUD_ORGANIZATION_ID
# are set in your environment or config_variables.yml
# your organization_id is indicated on https://app.greatexpectations.io/tokens page

# uncomment the next three lines to set them explicitly in this script
# import os
# os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<your_gx_cloud_access_token>"
# os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id_from_the_app>"

# Create a GX Data Context
context: CloudDataContext = gx.get_context(
    cloud_mode=True,
)

# Provide Datasource name
datasource_name = None
assert datasource_name, "Please set datasource_name."

# Get or add Datasource
datasource: Datasource = context.sources.add_or_update_pandas(datasource_name)

# Provide an Asset name
asset_name = None
assert asset_name, "Please set asset_name."

# Provide a path to data
path_to_data = None
# to use sample data uncomment next line
# path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
assert (
    path_to_data
), "Please set path_to_data. This can be a local filepath or a remote URL."

# Get or add Asset
try:
    asset: CSVAsset = datasource.get_asset(asset_name=asset_name)
except LookupError:
    asset: CSVAsset = datasource.add_csv_asset(
        asset_name, filepath_or_buffer=path_to_data
    )

# Build BatchRequest
batch_request: BatchRequest = asset.build_batch_request()

print(f"\n{20*'='}\nDatasource Config\n{20*'='}\n")
pprint.pprint(datasource.dict())

# Provide an Expectation Suite name
expectation_suite_name = None
assert expectation_suite_name, "Please set expectation_suite_name."

# Get or add Expectation Suite
expectation_suite: ExpectationSuite = context.add_or_update_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
expectation_suite_ge_cloud_id: str = expectation_suite.ge_cloud_id

# Add Expectations

# Set a column name you want to test here
column_name = None
# Uncomment the next line for a column name from sample data
# column_name = "passenger_count"
assert column_name is not None, "Please set column_name."

# Look up all expectations types here - https://greatexpectations.io/expectations/
expectation_configuration = gx.core.ExpectationConfiguration(
    **{
        "expectation_type": "expect_column_min_to_be_between",
        "kwargs": {"column": column_name, "min_value": 0.1},
        "meta": {},
    }
)

expectation_suite.add_expectation(expectation_configuration=expectation_configuration)

# Save the Expectation Suite
context.update_expectation_suite(expectation_suite=expectation_suite)

print(f"\n{20*'='}\nExpectation Suite\n{20*'='}\n")
pprint.pprint(expectation_suite)

# Provide a Checkpoint name
checkpoint_name = None
assert checkpoint_name, "Please set checkpoint_name."

checkpoint_config = {
    "name": checkpoint_name,
    "validations": [
        {
            "expectation_suite_name": expectation_suite_name,
            "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
            "batch_request": {
                "datasource_name": datasource.name,
                "data_asset_name": asset.name,
            },
        }
    ],
    "config_version": 1,
    "class_name": "Checkpoint",
}

checkpoint: Checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

print(f"\n{20*'='}\nCheckpoint Config\n{20*'='}\n")
pprint.pprint(checkpoint)

# Run the Checkpoint:
result = checkpoint.run()

print(f"\n{20*'='}\nValidation Result\n{20*'='}\n")
pprint.pprint(result)
