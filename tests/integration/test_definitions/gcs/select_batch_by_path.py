import great_expectations as gx

context = gx.get_context()

datasource_name = "my_gcs_datasource"
bucket_or_name = "test_docs_data"

datasource = context.data_sources.add_pandas_gcs(
    name=datasource_name, bucket_or_name=bucket_or_name, gcs_options={}
)

assert datasource_name in context.data_sources.all()

asset_name = "my_taxi_data_asset"
gcs_prefix = "data/taxi_yellow_tripdata_samples/"
data_asset = datasource.add_csv_asset(
    name=asset_name,
    gcs_prefix=gcs_prefix,
)

batch_definition = data_asset.add_batch_definition_path(
    "path", path="yellow_tripdata_sample_2019-02.csv"
)

my_batch_request = batch_definition.build_batch_request()
batches = data_asset.get_batch(my_batch_request)
assert batches.metadata == {
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv"
}
