import great_expectations as gx

context = gx.get_context()

datasource_name = "my_gcs_datasource"
bucket_or_name = "test_docs_data"

datasource = context.data_sources.add_pandas_gcs(
    name=datasource_name, bucket_or_name=bucket_or_name, gcs_options={}
)

assert datasource_name in context.datasources

asset_name = "my_taxi_data_asset"
gcs_prefix = "data/taxi_yellow_tripdata_samples/"
data_asset = datasource.add_csv_asset(
    name=asset_name,
    gcs_prefix=gcs_prefix,
)

batch_definition = data_asset.add_batch_definition_monthly(
    "monthly",
    regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
)

# not passing in batch parameters
my_batch_request = batch_definition.build_batch_request()
batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 3
assert batches[0].metadata == {
    "month": "01",
    "year": "2019",
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
}

# passing in batch parameters
my_batch_request = batch_definition.build_batch_request(
    batch_parameters={"year": "2019", "month": "02"}
)
batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 1
assert batches[0].metadata == {
    "month": "02",
    "year": "2019",
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv",
}
