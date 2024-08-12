import pathlib
import tempfile

import boto3

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.exceptions.exceptions import DataContextError

client = boto3.client("s3")
temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()
yaml: YAMLHandler = YAMLHandler()

# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py imports">
import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir=full_path_to_project_directory)
# </snippet>

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = pathlib.Path(
    full_path_to_project_directory, FileDataContext.GX_DIR, FileDataContext.GX_YML
)
great_expectations_yaml = yaml.load(great_expectations_yaml_file_path.read_text())

stores = great_expectations_yaml["stores"]
pop_stores = [
    "checkpoint_store",
    "validation_results_store",
    "validation_definition_store",
]
for store in pop_stores:
    stores.pop(store)

actual_existing_expectations_store = {}
actual_existing_expectations_store["stores"] = stores
actual_existing_expectations_store["expectations_store_name"] = great_expectations_yaml[
    "expectations_store_name"
]
expected_existing_expectations_store_yaml = """
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py existing_expectations_store">
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

expectations_store_name: expectations_store
# </snippet>
"""

assert actual_existing_expectations_store == yaml.load(
    expected_existing_expectations_store_yaml
)

# adding expectations store
configured_expectations_store_yaml = """
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py new_expectations_store">
stores:
  expectations_S3_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: '<YOUR S3 EXPECTATION BUCKET NAME>'
      prefix: '<YOUR S3 EXPECTATION PREFIX NAME>'  # Bucket and prefix in combination must be unique across all stores

expectations_store_name: expectations_S3_store
# </snippet>
"""

# replace example code with integration test configuration
configured_expectations_store = yaml.load(configured_expectations_store_yaml)
configured_expectations_store["stores"]["expectations_S3_store"]["store_backend"][
    "bucket"
] = "aws-golden-path-tests"
configured_expectations_store["stores"]["expectations_S3_store"]["store_backend"][
    "prefix"
] = "metadata/expectations"

# add and set the new expectation store
context.add_store(
    name=configured_expectations_store["expectations_store_name"],
    config=configured_expectations_store["stores"]["expectations_S3_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["expectations_store_name"] = "expectations_S3_store"
great_expectations_yaml["stores"]["expectations_S3_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

# adding validation results store
great_expectations_yaml_file_path = pathlib.Path(
    context.root_directory, FileDataContext.GX_YML
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

stores = great_expectations_yaml["stores"]
# popping the rest out so that we can do the comparison. They aren't going anywhere dont worry
pop_stores = [
    "checkpoint_store",
    "expectations_store",
    "expectations_S3_store",
    "validation_definition_store",
]
for store in pop_stores:
    stores.pop(store)

actual_existing_validation_results_store = {}
actual_existing_validation_results_store["stores"] = stores
actual_existing_validation_results_store["validation_results_store_name"] = (
    great_expectations_yaml["validation_results_store_name"]
)

expected_existing_validation_results_store_yaml = """
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py existing_validation_results_store">
stores:
  validation_results_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

validation_results_store_name: validation_results_store
# </snippet>
"""

assert actual_existing_validation_results_store == yaml.load(
    expected_existing_validation_results_store_yaml
)

# adding validations store
configured_validation_results_store_yaml = """
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py new_validation_results_store">
stores:
  validation_results_S3_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: '<YOUR S3 VALIDATION BUCKET NAME>'
      prefix: '<YOUR S3 VALIDATION PREFIX NAME>'  # Bucket and prefix in combination must be unique across all stores
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py set_new_validation_results_store">
validation_results_store_name: validation_results_S3_store
# </snippet>
"""

# replace example code with integration test configuration
configured_validation_results_store = yaml.load(
    configured_validation_results_store_yaml
)
configured_validation_results_store["stores"]["validation_results_S3_store"][
    "store_backend"
]["bucket"] = "aws-golden-path-tests"
configured_validation_results_store["stores"]["validation_results_S3_store"][
    "store_backend"
]["prefix"] = "metadata/validations"

# add and set the new validation store
context.add_store(
    name=configured_validation_results_store["validation_results_store_name"],
    config=configured_validation_results_store["stores"]["validation_results_S3_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["validation_results_store_name"] = "validation_results_S3_store"
great_expectations_yaml["stores"]["validation_results_S3_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

# adding data docs store
data_docs_site_yaml = """
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py add_data_docs_store">
data_docs_sites:
  local_site:
    class_name: SiteBuilder
    show_how_to_buttons: true
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
  S3_site:  # this is a user-selected name - you may select your own
    class_name: SiteBuilder
    store_backend:
      class_name: TupleS3StoreBackend
      bucket: '<YOUR S3 BUCKET NAME>'
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
# </snippet>
"""

data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR S3 BUCKET NAME>", "demo-data-docs"
)
great_expectations_yaml_file_path = pathlib.Path(
    context.root_directory, FileDataContext.GX_YML
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["data_docs_sites"] = yaml.load(data_docs_site_yaml)[
    "data_docs_sites"
]
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)


# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py add_s3_datasource">
datasource = context.data_sources.add_or_update_pandas_s3(
    name="s3_datasource", bucket="taxi-data-sample-test"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_pandas_s3_asset">
asset = datasource.add_csv_asset(
    name="csv_taxi_s3_asset",
)
batch_definition = asset.add_batch_definition_yearly(
    name="Yearly Taxi Data",
    regex=r".*_(?P<year>\d{4})\.csv",
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_batch_request">
batch_parameters = {"year": "2021"}
request = batch_definition.build_batch_request(batch_parameters=batch_parameters)
# </snippet>


# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_batch_list">
batch_parameters = {"year": "2021"}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
# </snippet>

config = context.fluent_datasources["s3_datasource"].yaml()
assert "name: s3_datasource" in config
assert "type: pandas_s3" in config

# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py get_validator">
try:
    context.suites.add(ExpectationSuite(name="test_suite"))
except DataContextError:
    ...
validator = context.get_validator(
    batch_request=request, expectation_suite_name="test_suite"
)

print(validator.head())
# </snippet>

# add expectations to validator
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py add_expectations">
validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)
# </snippet>

# build datadocs
# <snippet name="docs/docusaurus/docs/snippets/aws_cloud_storage_pandas.py build_docs">
context.build_data_docs()
# </snippet>
