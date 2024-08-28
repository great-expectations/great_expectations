import warnings

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import get_context
from great_expectations.data_context.types.base import (
    DataContextConfig,
    S3StoreBackendDefaults,
)

yaml = YAMLHandler()

# needed because GlueContext(sc) function emits the following FutureWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

s3_client = boto3.client("s3")
response = s3_client.get_object(
    Bucket="bucket", Key="bucket/great_expectations/great_expectations.yml"
)
config_file = yaml.load(response["Body"])

config = DataContextConfig(
    config_version=config_file["config_version"],
    datasources=config_file["datasources"],
    expectations_store_name=config_file["expectations_store_name"],
    validation_results_store_name=config_file["validation_results_store_name"],
    plugins_directory="/great_expectations/plugins",
    stores=config_file["stores"],
    data_docs_sites=config_file["data_docs_sites"],
    config_variables_file_path=config_file["config_variables_file_path"],
    anonymous_usage_statistics=config_file["anonymous_usage_statistics"],
    checkpoint_store_name=config_file["checkpoint_store_name"],
    store_backend_defaults=S3StoreBackendDefaults(
        default_bucket_name=config_file["data_docs_sites"]["s3_site"]["store_backend"]["bucket"]
    ),
)
context_gx = get_context(project_config=config)

expectation_suite_name = "suite_name"
suite = context_gx.suites.add(ExpectationSuite(name=expectation_suite_name))
batch_request = RuntimeBatchRequest(
    datasource_name="spark_s3",
    data_asset_name="datafile_name",
    batch_identifiers={"runtime_batch_identifier_name": "default_identifier"},
    data_connector_name="default_inferred_data_connector_name",
    runtime_parameters={"path": "s3a://bucket_name/path_to_file.format"},
)
validator = context_gx.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)
print(validator.head())
validator.expect_column_values_to_not_be_null(column="passenger_count")  ## add some test
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_config = {
    "class_name": "Checkpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}

checkpoint = context_gx.add_or_update_checkpoint(
    f"_tmp_checkpoint_{expectation_suite_name}", **checkpoint_config
)
results = checkpoint.run(result_format="SUMMARY", run_name="test")
validation_result_identifier = results.list_validation_result_identifiers()[0]
