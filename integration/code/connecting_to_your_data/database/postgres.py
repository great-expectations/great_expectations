from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.util import load_data_into_database

CONNECTION_STRING = """postgresql+psycopg2://postgres:@localhost/test_ci"""
load_data_into_database(
    "taxi_data",
    "../../../../integration/fixtures/data/reports/yellow_tripdata_sample_2019-01.csv",
    CONNECTION_STRING,
)

#context = ge.get_context()
context = ge.DataContext(
    context_root_dir="../../../../integration/fixtures/runtime_data_taxi_monthly/great_expectations"
)

# with a connection_string
config = """
name: my_postgres_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: <YOUR_CONNECTION_STRING_HERE>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       name: whole_table
"""
config = config.replace("<YOUR_CONNECTION_STRING_HERE>", CONNECTION_STRING)

print(f"Adding Datasource")
context.add_datasource(**yaml.load(config))

print(f"RuntimeBatchRequest")
# First test for RuntimeBatchRequest using a query
batch_request = ge.core.batch.RuntimeBatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"query": "SELECT * from taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "something_something"},
)

context.create_expectation_suite(expectation_suite_name="test_suite", overwrite_existing=True)
validator = context.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")
print(validator.head())

# Second test for BatchRequest naming a table
batch_request = ge.core.batch.BatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="taxi_data",  # this is the name of the table you want to retrieve

)
context.create_expectation_suite(expectation_suite_name="test_suite", overwrite_existing=True)
validator = context.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")
print(validator.head())
