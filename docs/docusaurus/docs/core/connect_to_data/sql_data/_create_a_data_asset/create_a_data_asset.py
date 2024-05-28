"""
To run this test locally, run:
1. Activate docker.
2. From the repo root dir, activate the postgresql database docker container:

pytest  --postgresql --docs-tests -k "create_a_datasource_postgres" tests/integration/test_script_runner.py
"""

# This section is setup for the environment used in the example script.
from tests.integration.db.taxi_data_utils import load_data_into_test_database

# add test_data to database for testing
load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string="postgresql+psycopg2://postgres:@localhost/test_ci",
)

# The example starts here
import great_expectations as gx

context = gx.get_context()

datasource_name = "my_new_datasource"
my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

data_source = context.data_sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)

print(context.get_datasource(datasource_name))

asset_name = "MY_TABLE_ASSET"
database_table_name = "postgres_taxi_data"
table_data_asset = data_source.add_table_asset(
    table_name=database_table_name, name=asset_name
)

asset_name = "MY_QUERY_ASSET"
asset_query = "SELECT * from postgres_taxi_data"
query_data_asset = data_source.add_query_asset(query=asset_query, name=asset_name)

bd_name = "FULL_TABLE"
batch_definition = table_data_asset.add_batch_definition_whole_table(name=bd_name)
batch_definition = query_data_asset.add_batch_definition_whole_table(name=bd_name)


batch = batch_definition.get_batch()
batch.head()
