import findspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)

# Set up spark session
findspark.init()
sc = SparkContext(appName="app")
spark = SparkSession(sc)


data = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]
df = spark.createDataFrame(data)

# TODO: fix this
# https://issues.apache.org/jira/browse/SPARK-27335?jql=text%20~%20%22setcallsite%22
# df.sql_ctx.sparkSession._jsparkSession = spark._jsparkSession
# df._sc = spark._sc
# Traceback:
# Calculating Metrics:  25%|██▌       | 2/8 [00:00<00:00, 17.63it/s]Traceback (most recent call last):
#   File "/Users/anthonyburdi/src/ge/great_expectations/great_expectations/execution_engine/execution_engine.py", line 315, in resolve_metrics
#     new_resolved = self.resolve_metric_bundle(metric_fn_bundle)
#   File "/Users/anthonyburdi/src/ge/great_expectations/great_expectations/execution_engine/sparkdf_execution_engine.py", line 676, in resolve_metric_bundle
#     res = df.agg(*aggregate_cols).collect()
#   File "/Users/anthonyburdi/Library/Local/SoftwareDevelopment/spark-3.1.2-bin-hadoop3.2/python/pyspark/sql/dataframe.py", line 676, in collect
#     with SCCallSiteSync(self._sc) as css:
#   File "/Users/anthonyburdi/Library/Local/SoftwareDevelopment/spark-3.1.2-bin-hadoop3.2/python/pyspark/traceback_utils.py", line 72, in __enter__
#     self._context._jsc.setCallSite(self._call_site)
# AttributeError: 'NoneType' object has no attribute 'setCallSite'


##################
# BEGIN CONTENT
##################

# 1. Set up Great Expectations
# install GE in notebook cell
# In-memory DataContext using DBFS and FilesystemStoreBackendDefaults

# This root directory is for use in Databricks
root_directory = "/dbfs/FileStore/"

# For testing purposes, we change the root_directory to an ephemeral location
# TODO: Is this necessary? Can we just remove the forward slash from `/dbfs/FileStore/`?
import os

root_directory = os.path.join(os.getcwd(), "dbfs_temp_directory")


data_context_config = DataContextConfig(
    # datasources={"my_spark_datasource": my_spark_datasource_config},
    store_backend_defaults=FilesystemStoreBackendDefaults(
        root_directory=root_directory
    ),
)
context = BaseDataContext(project_config=data_context_config)

# Check the stores were initialized
# TODO: Should this check the full tree?
assert os.listdir(root_directory) == ["checkpoints", "expectations", "uncommitted"]


# 2. Connect to your data
# Add a Datasource to our DataContext with data in a spark dataframe using RuntimeDataConnector and RuntimeBatchRequest with SparkDFExecutionEngine
#
#     (Use one of the available sample Databricks datasets to make this easily runnable as a tutorial)
# Example RuntimeDataConnector for use with a dataframe batch
# TODO: DatasourceConfig is not accepted by DataContext.add_datasource
# my_spark_datasource_config = DatasourceConfig(
#     class_name="Datasource",
#     execution_engine={"class_name": "SparkDFExecutionEngine"},
#     data_connectors={
#         "insert_your_runtime_data_connector_name_here": {
#             "module_name": "great_expectations.datasource.data_connector",
#             "class_name": "RuntimeDataConnector",
#             "batch_identifiers": [
#                 "some_key_maybe_pipeline_stage",
#                 "some_other_key_maybe_run_id"
#             ]
#         }
#     }
# )
# TODO: Consider changing this to use test_yaml_config() as in: https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py
my_spark_datasource_config = {
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "insert_your_runtime_data_connector_name_here": {
            "module_name": "great_expectations.datasource.data_connector",
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": [
                "some_key_maybe_pipeline_stage",
                "some_other_key_maybe_run_id",
            ],
        }
    },
}

context.add_datasource(
    name="insert_your_datasource_name_here", **my_spark_datasource_config
)

assert len(context.list_datasources()) == 1
assert context.list_datasources() == [
    {
        "module_name": "great_expectations.datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
        },
        "data_connectors": {
            "insert_your_runtime_data_connector_name_here": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": [
                    "some_key_maybe_pipeline_stage",
                    "some_other_key_maybe_run_id",
                ],
                "module_name": "great_expectations.datasource.data_connector",
            }
        },
        "name": "insert_your_datasource_name_here",
    }
]

# TODO: Should this be pulled from our repo during automated tests and only from the url for databricks notebooks as part of the guide?
data_file_url = "https://raw.githubusercontent.com/great-expectations/great_expectations/develop/tests/test_sets/taxi_yellow_trip_data_samples/yellow_trip_data_sample_2018-01.csv"
pandas_dataframe = pd.read_csv(data_file_url)
# df = spark.createDataFrame(data=pandas_dataframe)
#
batch_request_from_dataframe = RuntimeBatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_runtime_data_connector_name_here",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    batch_identifiers={
        "some_key_maybe_pipeline_stage": "prod",
        "some_other_key_maybe_run_id": "20211231-007",
    },
    runtime_parameters={"batch_data": df},  # Your dataframe goes here
)

# TODO: load data from local csv path, in DBFS. Here we will need to have two versions, one for display to the user and one for getting the data from our test runner
# batch_request_from_path =


# 3. Create expectations
#
#     Use a Validator and your batch of data to create an expectation suite interactively
#
expectation_suite_name = "insert_your_expectation_suite_name_here"
context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name, overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request_from_dataframe,
    expectation_suite_name=expectation_suite_name,
)

# validator.expect_column_values_to_not_be_null(column="vendor_id")
# validator.expect_column_values_to_be_between(column="congestion_surcharge", min_value=0, max_value=1000)
validator.expect_column_values_to_not_be_null(column="a")
validator.expect_column_values_to_be_between(column="b", min_value=1, max_value=10)

# Take a look at your suite
print(validator.get_expectation_suite(discard_failed_expectations=False))
# Save your suite to your expectation store
validator.save_expectation_suite(discard_failed_expectations=False)

# 4. Validate your data
#
#     Create a Checkpoint in-memory with your RuntimeBatchRequest as one of the validations and run it using Checkpoint.run()
#
#     View data docs

checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request_from_dataframe,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_{expectation_suite_name}", context, **checkpoint_config
)
checkpoint_result = checkpoint.run()

context.build_data_docs()

# validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
# context.open_data_docs(resource_identifier=validation_result_identifier)
