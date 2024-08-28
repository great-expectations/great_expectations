import os
import pathlib
import tempfile

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.exceptions.exceptions import DataContextError

temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()
yaml: YAMLHandler = YAMLHandler()

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py get_context">
import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir=full_path_to_project_directory)
# </snippet>

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
GCP_PROJECT_NAME = os.environ.get("GE_TEST_GCP_PROJECT")
if not GCP_PROJECT_NAME:
    raise ValueError(  # noqa: TRY003
        "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
    )
BIGQUERY_DATASET = "demo"


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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py existing_expectations_store">
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

expectations_store_name: expectations_store
"""

assert actual_existing_expectations_store == yaml.load(
    expected_existing_expectations_store_yaml
)


configured_expectations_store_yaml = """
stores:
  expectations_GCS_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>

expectations_store_name: expectations_GCS_store
"""

# replace example code with integration test configuration
configured_expectations_store = yaml.load(configured_expectations_store_yaml)
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "project"
] = GCP_PROJECT_NAME
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "bucket"
] = "test_metadata_store"
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "prefix"
] = "metadata/expectations"


# add and set the new expectation store
context.add_store(
    name=configured_expectations_store["expectations_store_name"],
    config=configured_expectations_store["stores"]["expectations_GCS_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["expectations_store_name"] = "expectations_GCS_store"
great_expectations_yaml["stores"]["expectations_GCS_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

# adding validation results store

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = pathlib.Path(
    context.root_directory, FileDataContext.GX_YML
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

stores = great_expectations_yaml["stores"]
# popping the rest out so taht we can do the comparison. They aren't going anywhere dont worry
pop_stores = [
    "checkpoint_store",
    "expectations_store",
    "expectations_GCS_store",
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
stores:
  validation_results_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

validation_results_store_name: validation_results_store
"""
assert actual_existing_validation_results_store == yaml.load(
    expected_existing_validation_results_store_yaml
)

configured_validation_results_store_yaml = """
stores:
  validation_results_GCS_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>

validation_results_store_name: validation_results_GCS_store
"""

# replace example code with integration test configuration
configured_validation_results_store = yaml.load(
    configured_validation_results_store_yaml
)
configured_validation_results_store["stores"]["validation_results_GCS_store"][
    "store_backend"
]["project"] = GCP_PROJECT_NAME
configured_validation_results_store["stores"]["validation_results_GCS_store"][
    "store_backend"
]["bucket"] = "test_metadata_store"
configured_validation_results_store["stores"]["validation_results_GCS_store"][
    "store_backend"
]["prefix"] = "metadata/validations"

# add and set the new validation store
context.add_store(
    name=configured_validation_results_store["validation_results_store_name"],
    config=configured_validation_results_store["stores"][
        "validation_results_GCS_store"
    ],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["validation_results_store_name"] = (
    "validation_results_GCS_store"
)
great_expectations_yaml["stores"]["validation_results_GCS_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

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
    "<YOUR GCP PROJECT NAME>", GCP_PROJECT_NAME
)
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCS BUCKET NAME>", "test_datadocs_store"
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

CONNECTION_STRING = f"bigquery://{GCP_PROJECT_NAME}/{BIGQUERY_DATASET}"
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py add_bigquery_datasource">
datasource = context.data_sources.add_or_update_sql(
    name="my_bigquery_datasource",
    connection_string="bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>",
)
# </snippet>

# For tests, we are replacing the `connection_string`
datasource = context.data_sources.add_or_update_sql(
    name="my_bigquery_datasource", connection_string=CONNECTION_STRING
)

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py add_bigquery_table_asset">
table_asset = datasource.add_table_asset(name="my_table_asset", table_name="taxi_data")
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py add_bigquery_query_asset">
query_asset = datasource.add_query_asset(
    name="my_query_asset", query="SELECT * from taxi_data"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py batch_request">
request = table_asset.build_batch_request()
# </snippet>


# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py add_or_update_expectation_suite">
try:
    context.suites.add(ExpectationSuite(name="test_bigquery_suite"))
except DataContextError:
    ...

validator = context.get_validator(
    batch_request=request, expectation_suite_name="test_bigquery_suite"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_bigquery.py validator_calls">
validator.expect_column_values_to_not_be_null(column="passenger_count")
validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)
# </snippet>
