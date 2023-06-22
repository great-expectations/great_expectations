import great_expectations as gx

context = gx.get_context()

datasource_name = "pandas_s3_example"
# nyc-tlc is hosted by Amazon Registry of Open Data on AWS: https://registry.opendata.aws/nyc-tlc-trip-records-pds/
# As of 2023-05-23 this note was posted: Note: access to this dataset is free,
# however direct S3 access does require an AWS account.
bucket_name = "nyc-tlc"
boto3_options = {"region_name": "us-east-1"}
datasource = context.sources.add_or_update_pandas_s3(
    name=datasource_name, bucket=bucket_name, boto3_options=boto3_options
)

batching_regex = r"green_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
s3_prefix = "csv_backup/"
asset_name = "taxi_data_aws_open_data"
data_asset = datasource.add_csv_asset(
    name=asset_name, batching_regex=batching_regex, s3_prefix=s3_prefix
)

print("data_asset.batch_request_options:", data_asset.batch_request_options)

batch_request = data_asset.build_batch_request(options={"year": "2019"})

batches = data_asset.get_batch_list_from_batch_request(batch_request)

print("len(batches):", len(batches))

for idx, batch in enumerate(batches):
    print(f"batch.batch_spec {idx + 1}:", batch.batch_spec)

batch_request_jan_2019 = data_asset.build_batch_request(
    options={"year": "2019", "month": "01"}
)

context.add_or_update_expectation_suite(expectation_suite_name="my_expectation_suite")
validator = context.get_validator(
    batch_request=batch_request_jan_2019,
    expectation_suite_name="my_expectation_suite",
)
print(validator.head())
print("columns:", validator.active_batch.data.dataframe.columns)

validator.expect_column_values_to_not_be_null("lpep_pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)

validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_quickstart_checkpoint",
    data_context=context,
    validator=validator,
)

checkpoint_result = checkpoint.run()

print(checkpoint_result.success)

assert checkpoint_result.success
