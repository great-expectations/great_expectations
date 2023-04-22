import pathlib
import tempfile

temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()


# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py imports">
import great_expectations as gx

context = gx.data_context.FileDataContext.create(full_path_to_project_directory)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py convert_to_file_context">
# context.convert_to_file_context()
# </snippet>

# # <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py add_expectation_store">
# context.add_store(
#     store_name="S3_expectations_store",
#     store_config={
#         "class_name": "ExpectationsStore",
#         "store_backend":{
#           "class_name": "TupleS3StoreBackend",
#           "bucket": "aws-golden-path-tests",
#           "prefix": "expectations"}
#     },
# )
# # </snippet>

# # <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py add_expectation_store">
# context.add_store(
#     store_name="S3_validation_store",
#     store_config={
#         "class_name": "ValidationsStore",
#         "store_backend":{
#           "class_name": "TupleS3StoreBackend",
#           "bucket": "aws-golden-path-tests", # this currently doesn't exist
#           "prefix": "validations"}
#     },
# )
# # </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py add_data_docs_store">
# adding data docs store
data_docs_site_yaml = """
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  gs_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
"""
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCP PROJECT NAME>", gcp_project
)
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCS BUCKET NAME>", "test_datadocs_store"
)
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["data_docs_sites"] = yaml.load(data_docs_site_yaml)[
    "data_docs_sites"
]
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py add_pandas_s3_datasource">
datasource = context.sources.add_or_update_pandas_s3(
    name="s3_datasource", bucket="taxi-data-sample-test"
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py get_pandas_s3_asset">
asset = datasource.add_csv_asset(
    name="csv_taxi_s3_asset",
    batching_regex=r".*_(?P<year>\d{4})\.csv",
)
# </snippet>

# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py get_batch_request">
request = asset.build_batch_request({"year": "2021"})
# </snippet>


# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_cloud_storage_pandas.py get_batch_list">
batches = asset.get_batch_list_from_batch_request(request)
# </snippet>

config = context.fluent_datasources["s3_datasource"].yaml()
assert "name: s3_datasource" in config
assert "type: pandas_s3" in config


# Validator
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=request, expectation_suite_name="test_suite"
)

print(validator.head())
validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>
# Checkpoint
# <snippet name="tests/integration/docusaurus/deployment_patterns/aws_emr_serverless_deployment_patterns.py validate">
checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_checkpoint",
    data_context=context,
    validations=[{"batch_request": request, "expectation_suite_name": "test_suite"}],
)

checkpoint_result = checkpoint.run()
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.build_data_docs()
# </snippet>
