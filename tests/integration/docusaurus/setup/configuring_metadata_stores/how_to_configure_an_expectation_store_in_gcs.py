import os

from ruamel import yaml

import great_expectations as ge

expected_existing_expectations_store_yaml = """
stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/
      
expectations_store_name: expectations_store
"""

configured_expectations_store_yaml = """
stores:
  expectations_GCS_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: '<YOUR_GCP_PROJECT_NAME>'
      bucket: '<YOUR_GCS_BUCKET_NAME>'
      prefix: '<YOUR_GCS_FOLDER_NAME>'
      
expectations_store_name: expectations_GCS_store
"""

copy_expectation_command = """
gsutil cp exp1.json gs://'<YOUR_GCS_BUCKET_NAME>'/'<YOUR_GCS_FOLDER_NAME>'
"""

copy_expectation_output = """
Operation completed over 1 objects/58.8 KiB.
"""

list_expectation_stores_command = """
great_expectations --v3-api store list
"""

list_expectation_stores_output = """
- name: expectations_store
class_name: ExpectationsStore
store_backend:
  class_name: TupleFilesystemStoreBackend
  base_directory: expectations/

- name: expectations_GCS_store
class_name: ExpectationsStore
store_backend:
  class_name: TupleGCSStoreBackend
  project: '<YOUR_GCP_PROJECT_NAME>'
  bucket: '<YOUR_GCS_BUCKET_NAME>'
  prefix: '<YOUR_GCS_FOLDER_NAME>'
"""

list_expectation_suites_command = """
great_expectations --v3-api suite list
"""

list_expectation_suites_output = """
1 Expectation Suite found:
 - exp1
"""


# NOTE: The following code is only for testing and can be ignored by users.
context = ge.get_context()

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path, "r") as f:
    great_expectations_yaml = yaml.safe_load(f)

stores = great_expectations_yaml["stores"]
pop_stores = ["checkpoint_store", "evaluation_parameter_store", "validations_store"]
for store in pop_stores:
    stores.pop(store)

actual_existing_expectations_store = dict()
actual_existing_expectations_store["stores"] = stores
actual_existing_expectations_store["expectations_store_name"] = great_expectations_yaml["expectations_store_name"]

assert actual_existing_expectations_store == yaml.safe_load(expected_existing_expectations_store_yaml)

# replace example code with integration test configuration
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCP_PROJECT_NAME>", ""
)
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCS_BUCKET_HERE>", "superconductive-integration-tests"
)
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCS_FOLDER_NAME>", "data/taxi_yellow_tripdata_samples"
)


