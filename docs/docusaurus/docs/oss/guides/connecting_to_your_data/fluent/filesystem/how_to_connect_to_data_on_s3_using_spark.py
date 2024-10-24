"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "how_to_connect_to_data_on_s3_using_spark" tests/integration/test_script_runner.py
```
"""

import great_expectations as gx

context = gx.get_context()

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_s3_using_spark.py define_add_spark_s3_args">
datasource_name = "my_s3_datasource"
bucket_name = "my_bucket"
boto3_options = {}
# </snippet>

bucket_name = "superconductive-docs-test"


# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_s3_using_spark.py create_datasource">
datasource = context.data_sources.add_spark_s3(
    name=datasource_name,
    bucket=bucket_name,
    boto3_options=boto3_options,
)
# </snippet>

assert datasource_name in context.data_sources.all()


# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/filesystem/how_to_connect_to_data_on_s3_using_spark.py add_asset">
asset_name = "my_taxi_data_asset"
s3_prefix = "data/taxi_yellow_tripdata_samples/"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
data_asset = datasource.add_csv_asset(
    name=asset_name,
    s3_prefix=s3_prefix,
    header=True,
    infer_schema=True,
)
batch_definition = data_asset.add_batch_definition_monthly(
    name="monthy_batch_definition",
    regex=batching_regex,
)
# </snippet>


batch = batch_definition.get_batch(batch_parameters={"year": "2019", "month": "03"})
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
