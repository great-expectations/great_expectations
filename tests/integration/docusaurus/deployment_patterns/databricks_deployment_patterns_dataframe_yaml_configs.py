# isort:skip_file
import os
import datetime

import pandas as pd
from ruamel import yaml

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

from great_expectations.core.util import get_or_create_spark_application

spark = get_or_create_spark_application()

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

# CODE vvvvv vvvvv
filename = "yellow_tripdata_sample_2019-01.csv"
data_dir = os.path.join(os.path.dirname(root_directory), "data")
pandas_df = pd.read_csv(os.path.join(data_dir, filename))
df = spark.createDataFrame(data=pandas_df)
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
assert len(pandas_df) == df.count() == 10000
assert len(pandas_df.columns) == len(df.columns) == 18
# ASSERTIONS ^^^^^ ^^^^^


# 4. Connect to your data

# CODE vvvvv vvvvv
my_spark_datasource_config = """
name: insert_your_datasource_name_here
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  insert_your_data_connector_name_here:
    module_name: great_expectations.datasource.data_connector
    class_name: RuntimeDataConnector
    batch_identifiers:
      - some_key_maybe_pipeline_stage
      - some_other_key_maybe_run_id
"""

context.test_yaml_config(my_spark_datasource_config)

context.add_datasource(**yaml.load(my_spark_datasource_config))

batch_request = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": f"my_run_name_{datetime.date.today().strftime('%Y%m%d')}",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)
# CODE ^^^^^ ^^^^^

# ASSERTIONS vvvvv vvvvv
assert len(context.list_datasources()) == 1
assert context.list_datasources()[0]["name"] == "insert_your_datasource_name_here"
assert list(context.list_datasources()[0]["data_connectors"].keys()) == [
    "insert_your_data_connector_name_here"
]

assert sorted(
    context.get_available_data_asset_names()["insert_your_datasource_name_here"][
        "insert_your_data_connector_name_here"
    ]
) == sorted([])
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

# ASSERTIONS vvvvv vvvvv
assert context.list_expectation_suite_names() == [expectation_suite_name]
suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
assert len(suite.expectations) == 2
# ASSERTIONS ^^^^^ ^^^^^

# 6. Validate your data (Dataframe)
# CODE vvvvv vvvvv
my_checkpoint_name = "insert_your_checkpoint_name_here"
my_checkpoint_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
"""

my_checkpoint = context.test_yaml_config(my_checkpoint_config)

context.add_checkpoint(**yaml.load(my_checkpoint_config))

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
)

# CODE ^^^^^ ^^^^^
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

# 7. Build and view Data Docs (Dataframe)
# CODE vvvvv vvvvv
# None, see guide
# CODE ^^^^^ ^^^^^
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
