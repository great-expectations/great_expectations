from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

import os

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)

stores = great_expectations_yaml["stores"]
pop_stores = ["checkpoint_store", "evaluation_parameter_store", "validations_store"]
for store in pop_stores:
    stores.pop(store)

actual_existing_expectations_store = {}
actual_existing_expectations_store["stores"] = stores
actual_existing_expectations_store["expectations_store_name"] = great_expectations_yaml[
    "expectations_store_name"
]

expected_existing_expectations_store_yaml = """
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

expectations_store_name: expectations_store
"""

assert actual_existing_expectations_store == yaml.safe_load(
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
configured_expectations_store = yaml.safe_load(configured_expectations_store_yaml)
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "project"
] = "support-sandbox-297615"
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "bucket"
] = "integration-test-sandbox-bucket"
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "prefix"
] = "metadata/expectations"

# try:
#     # remove this bucket if there was a failure in the script last time
#     result = subprocess.run(
#         "gsutil rm -r gs://superconductive-integration-tests/how_to_configure_an_expectation_store_in_gcs/expectations".split(),
#         check=True,
#         stderr=subprocess.PIPE,
#     )
# except Exception as e:
#     pass
#
# add and set the new expectation store
context.add_store(
    store_name=configured_expectations_store["expectations_store_name"],
    store_config=configured_expectations_store["stores"]["expectations_GCS_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)
great_expectations_yaml["expectations_store_name"] = "expectations_GCS_store"
great_expectations_yaml["stores"]["expectations_GCS_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f, default_flow_style=False)

# adding validation results store

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)

stores = great_expectations_yaml["stores"]
# popping the rest out so taht we can do the comparison. They aren't going anywhere dont worry
pop_stores = [
    "checkpoint_store",
    "evaluation_parameter_store",
    "expectations_store",
    "expectations_GCS_store",
]
for store in pop_stores:
    stores.pop(store)

actual_existing_validations_store = {}
actual_existing_validations_store["stores"] = stores
actual_existing_validations_store["validations_store_name"] = great_expectations_yaml[
    "validations_store_name"
]

expected_existing_validations_store_yaml = """
stores:
  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

validations_store_name: validations_store
"""
assert actual_existing_validations_store == yaml.safe_load(
    expected_existing_validations_store_yaml
)

configured_validations_store_yaml = """
stores:
  validations_GCS_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>

validations_store_name: validations_GCS_store
"""

# replace example code with integration test configuration
configured_validations_store = yaml.safe_load(configured_validations_store_yaml)
configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
    "project"
] = "support-sandbox-297615"
configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
    "bucket"
] = "integration-test-sandbox-bucket"
configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
    "prefix"
] = "metadata/validations"

# try:
#     # clean up validation store from last time if there was a failure mid-script
#     delete_validation_store_files = (
#         f"gsutil -m rm gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
#         + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}/**"
#     )
#     result = subprocess.run(
#         delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
#     )
#     stderr = result.stderr.decode("utf-8")
#     assert "Operation completed" in stderr
# except Exception as e:
#     pass
#
# add and set the new validation store
context.add_store(
    store_name=configured_validations_store["validations_store_name"],
    store_config=configured_validations_store["stores"]["validations_GCS_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)
great_expectations_yaml["validations_store_name"] = "validations_GCS_store"
great_expectations_yaml["stores"]["validations_GCS_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f, default_flow_style=False)

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
      prefix : <YOUR GCS PREFIX NAME>
    site_index_builder:
      class_name: DefaultSiteIndexBuilder
"""
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCP PROJECT NAME>", "support-sandbox-297615"
)
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCS BUCKET NAME>", "integration-test-sandbox-bucket"
)
data_docs_site_yaml = data_docs_site_yaml.replace(
    "<YOUR GCS PREFIX NAME>", "metadata/data_docs"
)
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)
great_expectations_yaml["data_docs_sites"] = yaml.safe_load(data_docs_site_yaml)[
    "data_docs_sites"
]
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

######
gcp_project = "support-sandbox-297615"
bigquery_dataset = "test_ci"

CONNECTION_STRING = f"bigquery://{gcp_project}/{bigquery_dataset}"

datasource_yaml = fr"""
name: my_bigquery_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       name: whole_table
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "bigquery://<GCP_PROJECT_NAME>/<BIGQUERY_DATASET>",
    CONNECTION_STRING,
)

context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))

# Test for RuntimeBatchRequest using a query. bigquery_temp_table name is passed in as batch_spec_passthrough
batch_request = RuntimeBatchRequest(
    datasource_name="my_bigquery_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="default_name",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from demo.taxi_data LIMIT 10"},
    batch_identifiers={"default_identifier_name": "default_identifier"},
    batch_spec_passthrough={
        "bigquery_temp_table": "ge_temp"
    },  # this is the name of the table you would like to use a 'temp_table'
)

context.create_expectation_suite(
    expectation_suite_name="yellow_tripdata_bigquery_suite", overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="yellow_tripdata_bigquery_suite"
)

validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

validator.save_expectation_suite(discard_failed_expectations=False)

my_checkpoint_name = "insert_your_checkpoint_name_here"
checkpoint_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: my_bigquery_datasource
      data_connector_name: default_runtime_data_connector_name
      data_asset_name: default_taxi_table
      batch_identifiers:
        default_identifier_name: 1
      runtime_parameters:
        query: SELECT * from demo.taxi_data LIMIT 10
      batch_speck_passthrough:
        bigquery_temp_table: ge_temp
    expectation_suite_name: yellow_tripdata_bigquery_suite
"""

context.add_checkpoint(**yaml.load(checkpoint_config))
checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
)
