import great_expectations as ge
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import Batch

YOUR_CREDENTIALS_HERE = """postgresql+psycopg2://postgres:@localhost/test_ci"""

context = ge.get_context()
# context = ge.DataContext(context_root_dir="../fixtures/runtime_data_taxi_monthly/great_expectations")

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

config = config.replace("<YOUR_CONNECTION_STRING_HERE>", YOUR_CREDENTIALS_HERE)

sanitize_yaml_and_save_datasource(context, config, overwrite_existing=True)

batch_request = ge.core.batch.RuntimeBatchRequest(
    datasource_name="my_postgres_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"query": "SELECT * FROM taxi_data LIMIT 5;"},
    batch_identifiers={"default_identifier_name": "something_something"},
)

# getting Batch
batch = context.get_batch(batch_request=batch_request)
assert isinstance(batch, Batch)
batch_data = batch.data
assert isinstance(
    batch_data, ge.execution_engine.sqlalchemy_batch_data.SqlAlchemyBatchData
)
