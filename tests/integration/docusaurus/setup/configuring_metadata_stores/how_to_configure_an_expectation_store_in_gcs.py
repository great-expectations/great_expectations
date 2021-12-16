import os

from ruamel import yaml

import great_expectations as ge

context = ge.get_context()

great_expectations_yaml_file_path = os.path.join(context.root_directory, "great_expectations.yml")

existing_expectations_store_yaml = """
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

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCP_PROJECT_NAME>", ""
)
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCS_BUCKET_HERE>", "superconductive-integration-tests"
)
configured_expectations_store_yaml = configured_expectations_store_yaml.replace(
    "<YOUR_GCS_FOLDER_NAME>", "data/taxi_yellow_tripdata_samples"
)

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