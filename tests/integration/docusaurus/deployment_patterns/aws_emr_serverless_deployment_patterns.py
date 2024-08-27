import boto3

from great_expectations.compatibility import pyspark
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import get_context
from great_expectations.data_context.types.base import (
    DataContextConfig,
    S3StoreBackendDefaults,
)
from great_expectations.execution_engine import SparkDFExecutionEngine

yaml = YAMLHandler()
# </snippet>

if __name__ == "__main__":
    ### critical part to reinitialize spark context
    sc = SparkDFExecutionEngine.get_or_create_spark_session()
    spark = pyspark.SQLContext(sc)

    spark_file = "pyspark_df.parquet"
    suite_name = "pandas_spark_suite"
    session = boto3.Session()
    s3_client = session.client("s3")
    response = s3_client.get_object(
        Bucket="bucket_name",
        Key="bucket_name/great_expectations/great_expectations.yml",
    )
    config_file = yaml.load(response["Body"])
    df_spark = spark.read.parquet("s3://bucket_name/data_folder/" + spark_file)

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
    expectation_suite_name = suite_name
    suite = context_gx.suites.get(suite_name)

    batch_request = RuntimeBatchRequest(
        datasource_name="spark_s3",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="datafile_name",
        batch_identifiers={"runtime_batch_identifier_name": "default_identifier"},
        runtime_parameters={"path": "s3a://bucket_name/path_to_file.format"},
    )
    validator = context_gx.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    print(validator.head())
    validator.expect_column_values_to_not_be_null(column="passenger_count")  ## add some test
    validator.save_expectation_suite(discard_failed_expectations=False)
    my_checkpoint_name = "in_memory_checkpoint"
    python_config = {
        "name": my_checkpoint_name,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
        ],
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "spark_s3",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "data_asset_name": "pyspark_df",
                },
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    }
    checkpoint = context_gx.add_or_update_checkpoint(**python_config)

    results = checkpoint.run(
        run_name="run_name",
        batch_request={
            "runtime_parameters": {"batch_data": df_spark},
            "batch_identifiers": {"runtime_batch_identifier_name": "default_identifier"},
        },
    )

    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context_gx.build_data_docs()
