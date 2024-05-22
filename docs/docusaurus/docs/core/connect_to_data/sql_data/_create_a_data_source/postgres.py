from tests.integration.db.taxi_data_utils import load_data_into_test_database

# add test_data to database for testing
load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string="postgresql+psycopg2://postgres:@localhost/test_ci",
)

import great_expectations as gx

context = gx.get_context()

datasource_name = "my_new_datasource"
# You only need to define one of these:
my_connection_string = (
    "postgresql+psycopg2://${USERNAME}:${PASSWORD}@<host>:<port>/<database>"
)
my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

datasource = context.sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)
