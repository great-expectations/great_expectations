import datetime
import logging
import os

import pytest

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

logger = logging.getLogger(__name__)


try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None
    logger.debug(
        "Unable to load GCS connection object; install optional google dependency for support"
    )

gcp_project: str = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
    )
bigquery_dataset: str = "demo"
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

batch_request = RuntimeBatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="asset_a",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from demo.taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
    # batch_spec_passthrough={"create_temp_table": False},
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
# add assertions
print(validator.head())


# temp_table_name
temp_table_name: str = validator.active_batch.data.selectable.description

client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference(project, bigquery_dataset)
table_ref = dataset_ref.table(temp_table_name)
table = client.get_table(table_ref)

expected_expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
    days=1
)
assert table.expires <= expected_expiration

# this will fire off a deprecation warning.
batch_request = RuntimeBatchRequest(
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
with pytest.raises(DeprecationWarning):
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
