"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "connect_to_data/filesystem/_amazon_s3/example_connect_using_pandas" tests/integration/test_script_runner.py
```
"""
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Full code example">
import great_expectations as gx

context = gx.get_context()

# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Datasource args">
datasource_name = "my_s3_datasource"
bucket_name = "my_bucket"
boto3_options = {}
# </snippet>

# bucket_name = "superconductive-docs-test"

# Python
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Create Data Source">
datasource = context.sources.add_pandas_s3(
    name=datasource_name,
    bucket=bucket_name,
    boto3_options=boto3_options
)
# </snippet>

assert datasource_name in context.datasources

# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Data Asset args">
asset_name = "my_taxi_data_asset"
s3_prefix = "data/taxi_yellow_tripdata_samples/"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
# </snippet>

# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Add Data Asset">
data_asset = datasource.add_csv_asset(
    name=asset_name, batching_regex=batching_regex, s3_prefix=s3_prefix
)
# </snippet>

assert data_asset
assert datasource.get_asset_names() == {"my_taxi_data_asset"}

# my_batch_request = data_asset.build_batch_request({"year": "2019", "month": "03"})
# batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
# assert len(batches) == 1
# assert set(batches[0].columns()) == {
#     "vendor_id",
#     "pickup_datetime",
#     "dropoff_datetime",
#     "passenger_count",
#     "trip_distance",
#     "rate_code_id",
#     "store_and_fwd_flag",
#     "pickup_location_id",
#     "dropoff_location_id",
#     "payment_type",
#     "fare_amount",
#     "extra",
#     "mta_tax",
#     "tip_amount",
#     "tolls_amount",
#     "improvement_surcharge",
#     "total_amount",
#     "congestion_surcharge",
# }
# </snippet>