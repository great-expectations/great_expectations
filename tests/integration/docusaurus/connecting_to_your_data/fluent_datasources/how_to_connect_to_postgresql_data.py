"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_postgresql_data" tests/integration/test_script_runner.py --postgresql
```
"""
import tests.test_utils as test_utils
import great_expectations as gx
import pathlib
import pandas as pd

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

# The following load & config blocks up until the batch requests are only to support testing.
PG_CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"


csv_path = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "yellow_tripdata_sample_2019-01.csv",
    ).resolve(strict=True)
)

load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path=csv_path,
    connection_string=PG_CONNECTION_STRING,
    load_full_dataset=True,
)

# Make sure the test data is as expected
df = pd.read_csv(csv_path)
assert df.passenger_count.unique().tolist() == [1, 2, 3, 4, 5, 6]
assert len(df) == 10000

context = gx.get_context()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgreql_data.py connection_string">
my_connection_string = (
    "postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>"
)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py connection_string2">
datasource_name = "my_datasource"
my_connection_string = (
    "postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>"
)
# </snippet>

my_connection_string = PG_CONNECTION_STRING

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_postgres">
datasource = context.sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_name">
asset_name = "my_table_asset"
asset_table_name = "postgres_taxi_data"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_table_asset">
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_query">
asset_name = "my_query_asset"
asset_query = "SELECT * from postgres_taxi_data"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_query_asset">
query_asset = datasource.add_query_asset(name=asset_name, query=asset_query)
# </snippet>

EXPECTED_ASSET_NAMES = {"my_table_asset", "my_query_asset"}
assert datasource.get_asset_names() == EXPECTED_ASSET_NAMES

for asset_name in EXPECTED_ASSET_NAMES:
    asset = datasource.get_asset(asset_name)
    assert asset

    my_batch_request = asset.build_batch_request()
    batches = asset.get_batch_list_from_batch_request(my_batch_request)
    assert len(batches) == 1
    assert set(batches[0].columns()) == {
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    }
