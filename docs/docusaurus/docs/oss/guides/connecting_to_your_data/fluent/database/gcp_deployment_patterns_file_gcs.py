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

yaml = YAMLHandler()

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py get_context">
import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir=full_path_to_project_directory)
# </snippet>


# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(  # noqa: TRY003
        "Environment Variable GE_TEST_GCP_PROJECT is required to run GCS integration tests"
    )

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = pathlib.Path(
    context.root_directory, FileDataContext.GX_YML
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py expected_expectation_store">
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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py new_expectation_store">
stores:
  expectations_GCS_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>

expectations_store_name: expectations_GCS_store
# </snippet>
"""

# replace example code with integration test configuration
configured_expectations_store = yaml.load(configured_expectations_store_yaml)
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "project"
] = gcp_project
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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py expected_validation_results_store">
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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py new_validation_results_store">
stores:
  validation_results_GCS_store:
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>

validation_results_store_name: validation_results_GCS_store
# </snippet>
"""

# replace example code with integration test configuration
configured_validation_results_store = yaml.load(
    configured_validation_results_store_yaml
)
configured_validation_results_store["stores"]["validation_results_GCS_store"][
    "store_backend"
]["project"] = gcp_project
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
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py new_data_docs_store">
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
# </snippet>
"""
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCP PROJECT NAME>", gcp_project
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

# adding datasource
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py datasource">
datasource = context.data_sources.add_pandas_gcs(
    name="gcs_datasource", bucket_or_name="test_docs_data"
)
# </snippet>

### Add GCS data to the Datasource as a Data Asset
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py prefix_and_batching_regex">
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
prefix = "data/taxi_yellow_tripdata_samples/"
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py asset">
data_asset = datasource.add_csv_asset(name="csv_taxi_gcs_asset", gcs_prefix=prefix)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py batch_request">
batch_definition = data_asset.add_batch_definition_monthly(
    name="Monthly Taxi Data", regex=batching_regex
)
batch_request = batch_definition.build_batch_request(
    batch_parameters={
        "month": "03",
    }
)
# </snippet>

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py add_expectation_suite">
try:
    context.suites.add(ExpectationSuite(name="test_gcs_suite"))
except DataContextError:
    ...

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_gcs_suite"
)
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)

# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/gcp_deployment_patterns_file_gcs.py validator_calls">
validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=-3, max_value=1000
)
# </snippet>
