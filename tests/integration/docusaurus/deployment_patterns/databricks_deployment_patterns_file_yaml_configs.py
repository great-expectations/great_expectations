import os

from ruamel import yaml

from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

# 1. Install Great Expectations
# %pip install great-expectations
# Imports

# 2. Set up Great Expectations
# In-memory DataContext using DBFS and FilesystemStoreBackendDefaults

# CODE vvvvv vvvvv
# This root directory is for use in Databricks
root_directory = "/dbfs/great_expectations/"

# For testing purposes only, we change the root_directory to an ephemeral location created by our test runner
root_directory = os.path.join(os.getcwd(), "dbfs_temp_directory")

data_context_config = DataContextConfig(
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
# Check the stores were initialized
uncommitted_directory = os.path.join(root_directory, "uncommitted")
assert {"checkpoints", "expectations", "uncommitted"}.issubset(
    set(os.listdir(root_directory))
)
assert os.listdir(uncommitted_directory) == ["validations"]
# ASSERTIONS ^^^^^ ^^^^^

# 3. Prepare your data

# See guide

# 4. Connect to your data

# CODE vvvvv vvvvv
my_spark_datasource_config = r"""
name: insert_your_datasource_name_here
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: SparkDFExecutionEngine
data_connectors:
  insert_your_data_connector_name_here:
    module_name: great_expectations.datasource.data_connector
    class_name: InferredAssetDBFSDataConnector
    base_directory: /dbfs/example_data/nyctaxi/tripdata/yellow/
    glob_directive: "*.csv.gz"
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)_(\d{4})-(\d{2})\.csv\.gz
"""

# For this test script, change base_directory to location where test runner data is located
my_spark_datasource_config = my_spark_datasource_config.replace(
    "/dbfs/example_data/nyctaxi/tripdata/yellow/",
    os.path.join(root_directory, "../data/"),
)
# For this test script, change the data_conector class_name to the filesystem version since we are unable to
# mock /dbfs/ and dbfs:/ style paths without using a mocked filesystem
my_spark_datasource_config = my_spark_datasource_config.replace(
    "InferredAssetDBFSDataConnector", "InferredAssetFilesystemDataConnector"
)
# For this test script, we use uncompressed sample csv files, but in the databricks example these files are compressed
my_spark_datasource_config = my_spark_datasource_config.replace(
    r"(.*)_(\d{4})-(\d{2})\.csv\.gz", r"(.*)_(\d{4})-(\d{2})\.csv"
)
my_spark_datasource_config = my_spark_datasource_config.replace("*.csv.gz", "*.csv")

# Data location used when running or debugging this script directly
# os.path.join(
#     root_directory, "../../../../test_sets/taxi_yellow_tripdata_samples/first_3_files/"
# )

context.test_yaml_config(my_spark_datasource_config)

context.add_datasource(**yaml.safe_load(my_spark_datasource_config))

batch_request = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="yellow_tripdata",
    batch_spec_passthrough={
        "reader_method": "csv",
        "reader_options": {
            "header": True,
        },
    },
)

# For the purposes of this script, the data_asset_name includes "sample"
batch_request.data_asset_name = "yellow_tripdata_sample"
# CODE ^^^^^ ^^^^^

# NOTE: The following code is only for testing and can be ignored by users.
# ASSERTIONS vvvvv vvvvv
assert len(context.list_datasources()) == 1
assert context.list_datasources()[0]["name"] == "insert_your_datasource_name_here"
assert list(context.list_datasources()[0]["data_connectors"].keys()) == [
    "insert_your_data_connector_name_here"
]

sorted_available_data_asset_names_from_datasource = sorted(
    context.datasources[
        "insert_your_datasource_name_here"
    ].get_available_data_asset_names(
        data_connector_names="insert_your_data_connector_name_here"
    )[
        "insert_your_data_connector_name_here"
    ]
)
sorted_available_data_asset_names_from_context = sorted(
    context.get_available_data_asset_names()["insert_your_datasource_name_here"][
        "insert_your_data_connector_name_here"
    ]
)

assert (
    len(sorted_available_data_asset_names_from_datasource)
    == len(sorted_available_data_asset_names_from_context)
    == 1
)
assert (
    sorted_available_data_asset_names_from_datasource
    == sorted_available_data_asset_names_from_context
    == sorted(
        [
            "yellow_tripdata_sample",
        ]
    )
)
# ASSERTIONS ^^^^^ ^^^^^

# 5. Create expectations
# CODE vvvvv vvvvv
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

validator.save_expectation_suite(discard_failed_expectations=False)
# CODE ^^^^^ ^^^^^

# NOTE: The following code is only for testing and can be ignored by users.
# ASSERTIONS vvvvv vvvvv
assert context.list_expectation_suite_names() == [expectation_suite_name]
suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
assert len(suite.expectations) == 2
# ASSERTIONS ^^^^^ ^^^^^

# 6. Validate your data
# CODE vvvvv vvvvv
my_checkpoint_name = "insert_your_checkpoint_name_here"
checkpoint_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: insert_your_datasource_name_here
      data_connector_name: insert_your_data_connector_name_here
      data_asset_name: yellow_tripdata
      data_connector_query:
        index: -1
      batch_spec_passthrough:
        reader_method: csv
        reader_options:
          header: True
    expectation_suite_name: {expectation_suite_name}
"""

# For the purposes of this script, the data_asset_name includes "sample"
checkpoint_config = checkpoint_config.replace(
    "yellow_tripdata", "yellow_tripdata_sample"
)

my_checkpoint = context.test_yaml_config(checkpoint_config)

context.add_checkpoint(**yaml.safe_load(checkpoint_config))

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
)
# CODE ^^^^^ ^^^^^

# NOTE: The following code is only for testing and can be ignored by users.
# ASSERTIONS vvvvv vvvvv
assert checkpoint_result.checkpoint_config["name"] == my_checkpoint_name
assert not checkpoint_result.success
first_validation_result_identifier = (
    checkpoint_result.list_validation_result_identifiers()[0]
)
first_run_result = checkpoint_result.run_results[first_validation_result_identifier]
assert (
    first_run_result["validation_result"]["statistics"]["successful_expectations"] == 1
)
assert (
    first_run_result["validation_result"]["statistics"]["unsuccessful_expectations"]
    == 1
)
assert (
    first_run_result["validation_result"]["statistics"]["evaluated_expectations"] == 2
)
# ASSERTIONS ^^^^^ ^^^^^

# 7. Build and view Data Docs
# CODE vvvvv vvvvv
# None, see guide
# CODE ^^^^^ ^^^^^

# NOTE: The following code is only for testing and can be ignored by users.
# ASSERTIONS vvvvv vvvvv
# Check that validations were written to the store
data_docs_local_site_path = os.path.join(
    root_directory, "uncommitted", "data_docs", "local_site"
)
assert sorted(os.listdir(data_docs_local_site_path)) == sorted(
    ["index.html", "expectations", "validations", "static"]
)
assert os.listdir(os.path.join(data_docs_local_site_path, "validations")) == [
    expectation_suite_name
], "Validation was not written successfully to Data Docs"

run_name = first_run_result["validation_result"]["meta"]["run_id"].run_name
assert (
    len(
        os.listdir(
            os.path.join(
                data_docs_local_site_path,
                "validations",
                expectation_suite_name,
                run_name,
            )
        )
    )
    == 1
), "Validation was not written successfully to Data Docs"
# ASSERTIONS ^^^^^ ^^^^^
