import re

import great_expectations as gx

context = gx.get_context()

datasource_name = "my_s3_datasource"
bucket_name = "superconductive-docs-test"

datasource = context.data_sources.add_pandas_s3(
    name=datasource_name, bucket=bucket_name, boto3_options={}
)

asset_name = "my_taxi_data_asset"
s3_prefix = "data/taxi_yellow_tripdata_samples/"
data_asset = datasource.add_csv_asset(name=asset_name, s3_prefix=s3_prefix)

batching_regex = re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
batch_definition = data_asset.add_batch_definition_monthly("monthly", regex=batching_regex)

# Get all batches by month
batch_request = batch_definition.build_batch_request()
batch_list = data_asset.get_batch_list_from_batch_request(batch_request)
assert len(batch_list) == 3
assert batch_list[0].metadata == {
    "year": "2019",
    "month": "01",
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
}

# Get a specific batch by month
batch_request = batch_definition.build_batch_request({"year": "2019", "month": "02"})
batch_list = data_asset.get_batch_list_from_batch_request(batch_request)
assert len(batch_list) == 1
assert batch_list[0].metadata == {
    "year": "2019",
    "month": "02",
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv",
}
