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


context = ge.get_context()

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path, "r") as f:
    great_expectations_yaml = yaml.safe_load(f)

stores_dict = great_expectations_yaml["stores"]
pop_stores = ["checkpoint_store", "evaluation_parameter_store", "validations_store"]
for store in pop_stores:
    stores_dict.pop(store)
stores_yaml = yaml.dump({"stores": stores_dict}, default_flow_style=False)
expectations_store_name_yaml = "expectations_store_name: " + yaml.dump(
    great_expectations_yaml["expectations_store_name"]
)
actual_existing_expectations_store_yaml = (
    stores_yaml + "\n" + expectations_store_name_yaml
)

# yaml load/dump round-trip is required due to parser alphabetization
assert yaml.dump(yaml.safe_load(actual_existing_expectations_store_yaml)) == yaml.dump(
    yaml.safe_load(expected_existing_expectations_store_yaml)
)
