"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_data_on_azure_blob_storage_using_pandas" tests/integration/test_script_runner.py
```
"""

import os

import great_expectations as gx

context = gx.get_context()

os.environ["AZURE_STORAGE_ACCOUNT_URL"] = "superconductivetesting.blob.core.windows.net"

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py define_add_pandas_abs_args">
datasource_name = "my_datasource"
azure_options = {
    "account_url": "${AZURE_STORAGE_ACCOUNT_URL}",
    "credential": "${AZURE_CREDENTIAL}",
}
# </snippet>

bucket_or_name = "test_docs_data"

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py create_datasource">
datasource = context.sources.add_pandas_abs(
    name=datasource_name, azure_options=azure_options
)
# </snippet>

assert datasource_name in context.datasources

asset_name = "my_taxi_data_asset"
abs_container = "superconductive-public"
abs_name_starts_with = "data/taxi_yellow_tripdata_samples/"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_azure_blob_storage_using_pandas.py add_asset">
data_asset = datasource.add_csv_asset(
    name=asset_name,
    batching_regex=batching_regex,
    abs_container=abs_container,
    abs_name_starts_with=abs_name_starts_with,
)
# </snippet>

assert data_asset

assert datasource.get_asset_names() == {"my_taxi_data_asset"}

my_batch_request = data_asset.build_batch_request({"year": "2019", "month": "03"})
batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
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
