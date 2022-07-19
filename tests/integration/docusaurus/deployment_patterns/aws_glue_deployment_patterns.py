from pyspark.context import SparkContext
from awsglue.context import GlueContext
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import RuntimeBatchRequest
import boto3
import yaml

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket='bucket',Key="bucket/great_expectations/great_expectations.yml")
config_file = yaml.safe_load(response["Body"])

config = DataContextConfig(config_version=config_file['config_version'], datasources=config_file['datasources'],
                           expectations_store_name=config_file['expectations_store_name'],
                           validations_store_name=config_file['validations_store_name'],
                           evaluation_parameter_store_name=config_file['evaluation_parameter_store_name'],
                           plugins_directory='/great_expectations/plugins',
                           validation_operators=config_file['validation_operators'],
                           stores=config_file['stores'],
                           data_docs_sites=config_file['data_docs_sites'],
                           config_variables_file_path=config_file['config_variables_file_path'],
                           anonymous_usage_statistics=config_file['anonymous_usage_statistics'],
                           store_backend_defaults=S3StoreBackendDefaults(default_bucket_name=config_file['data_docs_sites']['s3_site']['store_backend']['bucket']))
context_ge = BaseDataContext(project_config=config)

expectation_suite_name = 'suite_name'
suite = context_ge.create_expectation_suite(expectation_suite_name)
batch_request = RuntimeBatchRequest(datasource_name="spark_s3",data_connector_name="default_inferred_data_connector_name",
                             runtime_parameters={"path": "s3a://bucket_name/path_to_file.format"}
)
validator = context_ge.get_validator(
   batch_request=batch_request,
   expectation_suite_name=expectation_suite_name,
)
print(validator.head())
validator.expect_column_values_to_not_be_null(column="passenger_count") ## add some test
validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint_config = {"class_name": "SimpleCheckpoint",
                     "validations": [{"batch_request": batch_request,"expectation_suite_name": expectation_suite_name}]
   }

checkpoint = SimpleCheckpoint(f"_tmp_checkpoint_{expectation_suite_name}",context_ge,**checkpoint_config)
results = checkpoint.run(result_format="SUMMARY", run_name="test")
validation_result_identifier = results.list_validation_result_identifiers()[0]