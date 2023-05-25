"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_a_sql_table" tests/integration/test_script_runner.py
```
"""
import tests.test_utils as test_utils
import great_expectations as gx
import pathlib
import great_expectations as gx

sqlite_database_path = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    ).resolve(strict=True)
)


my_table_name = "yellow_tripdata_sample_2019_01"

context = gx.get_context()

connection_string = f"sqlite:///{sqlite_database_path}"

datasource = context.sources.add_sql(
    name="my_datasource", connection_string=connection_string
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py datasource">
datasource = context.get_datasource("my_datasource")
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py create_datasource">
table_asset = datasource.add_table_asset(name="my_asset", table_name=my_table_name)
# </snippet>

assert datasource.get_asset_names() == {"my_asset"}

my_asset = datasource.get_asset("my_asset")
assert my_asset

my_batch_request = my_asset.build_batch_request()
batches = my_asset.get_batch_list_from_batch_request(my_batch_request)
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
