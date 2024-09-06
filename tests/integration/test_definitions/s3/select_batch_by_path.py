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

batch_definition = data_asset.add_batch_definition_path(
    "path",
    path="yellow_tripdata_sample_2019-03.csv",
)

batch_request = batch_definition.build_batch_request()
batch_list = data_asset.get_batch(batch_request)

assert batch_list.metadata == {
    "path": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-03.csv"
}
