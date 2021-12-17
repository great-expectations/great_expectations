import os
import subprocess

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
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>
      
expectations_store_name: expectations_GCS_store
"""

copy_expectation_command = """
gsutil cp expectations/my_expectation_suite.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/my_expectation_suite.json
"""

copy_expectation_output = """
Operation completed over 1 objects
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
  project: <YOUR GCP PROJECT NAME>
  bucket: <YOUR GCS BUCKET NAME>
  prefix: <YOUR GCS PREFIX NAME>
"""

list_expectation_suites_command = """
great_expectations --v3-api suite list
"""

list_expectation_suites_output = """
1 Expectation Suite found:
 - my_expectation_suite
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
actual_existing_expectations_store["expectations_store_name"] = great_expectations_yaml[
    "expectations_store_name"
]

assert actual_existing_expectations_store == yaml.safe_load(
    expected_existing_expectations_store_yaml
)

# replace example code with integration test configuration
configured_expectations_store = yaml.safe_load(configured_expectations_store_yaml)
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "project"
] = "superconductive-internal/"
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "bucket"
] = "superconductive-integration-tests/"
configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
    "prefix"
] = "data/taxi_yellow_tripdata_samples/"

# add and set the new expectation store
context.add_store(
    store_name=configured_expectations_store["expectations_store_name"],
    store_config=configured_expectations_store["stores"]["expectations_GCS_store"],
)
with open(great_expectations_yaml_file_path, "r") as f:
    great_expectations_yaml_str = f.read()
great_expectations_yaml_str = great_expectations_yaml_str.replace(
    "expectations_store_name: expectations_store",
    "expectations_store_name: expectations_GCS_store",
)
with open(great_expectations_yaml_file_path, "w") as f:
    f.write(great_expectations_yaml_str)

expectation_suite_name = "my_expectation_suite"
context.create_expectation_suite(expectation_suite_name=expectation_suite_name)

# try gsutil cp command
local_expectation_suite_file_path = os.path.join(
    context.root_directory, "expectations", f"{expectation_suite_name}.json"
)
copy_expectation_command = copy_expectation_command.replace(
    "expectations/my_expectation_suite.json", local_expectation_suite_file_path
)
copy_expectation_command = copy_expectation_command.replace(
    "<YOUR GCS BUCKET NAME>",
    configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
        "bucket"
    ],
)
copy_expectation_command = copy_expectation_command.replace(
    "<YOUR GCS PREFIX NAME>/my_expectation_suite.json",
    configured_expectations_store["stores"]["expectations_GCS_store"]["store_backend"][
        "prefix"
    ]
    + f"{expectation_suite_name}.json",
)

result = subprocess.run(
    copy_expectation_command.strip().split(), check=True, stderr=subprocess.PIPE
)
stderr = result.stderr.decode("utf-8")
assert copy_expectation_output.strip() in stderr

# list expectation stores
print(great_expectations_yaml_str)
result = subprocess.run(
    list_expectation_stores_command.strip().split(), check=True, stdout=subprocess.PIPE
)
stdout = result.stdout.decode("utf-8")
print(stdout)
assert stdout == list_expectation_stores_output
