import datetime
import os

import pytest
from google.cloud import bigquery

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.validator.validator import Validator

"""
What does this test and why?

This integration test is part of the deprecation of the bigquery_temp_table parameter. As of GE 0.15.3,
tables that are created as the result of a query are created as permanent tables with an expiration of 24 hours, with
more information to be found:

- https://github.com/great-expectations/great_expectations/pull/4925
- https://stackoverflow.com/questions/20673986/how-to-create-temporary-table-in-google-bigquery

This integration test tests the following:

1. get_validator() will create a temp_table with the expected expiration
2. passing in `bigquery_temp_table` as part of `batch_spec_passthrough` will raise a DeprecationWarning.

"""

gcp_project: str = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
    )
bigquery_dataset: str = "test_ci"
CONNECTION_STRING: str = f"bigquery://{gcp_project}/{bigquery_dataset}"

yaml = YAMLHandler()

context: DataContext = ge.get_context()

datasource_yaml: str = f"""
name: my_bigquery_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       assets:
            asset_a:
                batch_identifiers:
                    - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       include_schema_name: true
"""

datasource_yaml: str = datasource_yaml.replace(
    "bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>",
    CONNECTION_STRING,
)

context.add_datasource(**yaml.load(datasource_yaml))

batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="asset_a",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from demo.taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator: Validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
assert validator

# What is the name of the temp_table that was created as part of `get_validator`?
temp_table_name: str = validator.active_batch.data.selectable.description
client: bigquery.Client = bigquery.Client()
project: str = client.project
dataset_ref: bigquery.DatasetReference = bigquery.DatasetReference(
    project, bigquery_dataset
)
table_ref: bigquery.TableReference = dataset_ref.table(temp_table_name)
table: bigquery.Table = client.get_table(table_ref)

# what is its expiration
expected_expiration: datetime.datetime = datetime.datetime.now(
    datetime.timezone.utc
) + datetime.timedelta(days=1)
assert table.expires <= expected_expiration

# Ensure that passing in `bigquery_temp_table` will fire off a DeprecationWarning
batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="asset_a",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from demo.taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
    batch_spec_passthrough={
        "bigquery_temp_table": "ge_temp"
    },  # this is the name of the table you would like to use a 'temp_table'
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
with pytest.warns(DeprecationWarning):
    validator: Validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
