import os

import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.util import gen_directory_tree_str

spark = get_or_create_spark_application()

# TODO: ABOVE THIS LINE HAS NOT BEEN CLEANED UP
# ---------------------------------------------

##################
# BEGIN CONTENT
##################

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
assert os.listdir(root_directory) == ["checkpoints", "expectations", "uncommitted"]
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
    class_name: InferredAssetFilesystemDataConnector
    base_directory: /dbfs/example_data/nyctaxi/tripdata/yellow/
    glob_directive: "*.csv"
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)_(\d{4})-(\d{2})\.csv
"""

# # For this test script, change base_directory to location where test runner data is located
my_spark_datasource_config = my_spark_datasource_config.replace(
    "/dbfs/example_data/nyctaxi/tripdata/yellow/",
    os.path.join(root_directory, "../data/"),
)

# Temporary location for running this file (e.g. during debugging)
# ] = os.path.join(
#     root_directory, "../../../../test_sets/taxi_yellow_tripdata_samples/first_3_files/"
# )


# Yaml Version
context.test_yaml_config(my_spark_datasource_config)

context.add_datasource(**yaml.load(my_spark_datasource_config))

batch_request = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="yellow_tripdata_sample",
    batch_spec_passthrough={
        "reader_method": "csv",
        "reader_options": {
            "header": True,
        },
    },
)
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
      data_asset_name: yellow_tripdata_sample
      data_connector_query:
        index: -1
      batch_spec_passthrough:
        reader_method: csv
        reader_options:
          header: True
    expectation_suite_name: {expectation_suite_name}
"""

my_checkpoint = context.test_yaml_config(checkpoint_config)

context.add_checkpoint(**yaml.load(checkpoint_config))

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
