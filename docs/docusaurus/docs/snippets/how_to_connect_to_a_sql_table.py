"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "how_to_connect_to_a_sql_table" tests/integration/test_script_runner.py
```
"""

import pathlib
import warnings

import great_expectations as gx
from great_expectations.core.partitioners import PartitionerColumnValue
from great_expectations.datasource.fluent import GxDatasourceWarning

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

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=GxDatasourceWarning)
    datasource = context.data_sources.add_sql(
        name="my_datasource", connection_string=connection_string
    )

# Python
# <snippet name="docs/docusaurus/docs/snippets/how_to_connect_to_a_sql_table.py datasource">
datasource = context.data_sources.get("my_datasource")
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/snippets/how_to_connect_to_a_sql_table.py create_datasource">
table_asset = datasource.add_table_asset(name="my_asset", table_name=my_table_name)
# </snippet>

assert datasource.get_asset_names() == {"my_asset"}

my_asset = datasource.get_asset("my_asset")
assert my_asset

my_batch_request = my_asset.build_batch_request()
batch = my_asset.get_batch(my_batch_request)
assert set(batch.columns()) == {
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

# <snippet name="docs/docusaurus/docs/snippets/how_to_connect_to_a_sql_table.py add_vendor_id_splitter">
partitioner = PartitionerColumnValue(column_name="vendor_id")
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/how_to_connect_to_a_sql_table.py build_vendor_id_batch_request">
my_batch_request = my_asset.build_batch_request(
    options={"vendor_id": 1},
    partitioner=partitioner,
)
# </snippet>

batch = my_asset.get_batch(my_batch_request)
