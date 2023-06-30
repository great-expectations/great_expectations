import os
import subprocess

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery integration tests"
    )

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

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
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml">
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

configured_expectations_store_yaml = """
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml">
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
] = "how_to_configure_an_expectation_store_in_gcs/expectations"

try:
    # remove this bucket if there was a failure in the script last time
    result = subprocess.run(
        "gsutil rm -r gs://test_metadata_store/how_to_configure_an_expectation_store_in_gcs/expectations".split(),
        check=True,
        stderr=subprocess.PIPE,
    )
except Exception as e:
    pass

# add and set the new expectation store
context.add_store(
    store_name=configured_expectations_store["expectations_store_name"],
    store_config=configured_expectations_store["stores"]["expectations_GCS_store"],
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)
great_expectations_yaml["expectations_store_name"] = "expectations_GCS_store"
great_expectations_yaml["stores"]["expectations_GCS_store"]["store_backend"].pop(
    "suppress_store_backend_id"
)
with open(great_expectations_yaml_file_path, "w") as f:
    yaml.dump(great_expectations_yaml, f)

expectation_suite_name = "my_expectation_suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)


"""
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command">
gsutil cp expectations/my_expectation_suite.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/my_expectation_suite.json
# </snippet>
"""

# Override without snippet tag
# try gsutil cp command
copy_expectation_command = """
gsutil cp expectations/my_expectation_suite.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/my_expectation_suite.json
"""

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
    + f"/{expectation_suite_name}.json",
)

result = subprocess.run(
    copy_expectation_command.strip().split(),
    check=True,
    stderr=subprocess.PIPE,
)
stderr = result.stderr.decode("utf-8")

"""
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output">
Operation completed over 1 objects
# </snippet>
"""

# Override without snippet tag
copy_expectation_output = """
Operation completed over 1 objects
"""

assert copy_expectation_output.strip() in stderr

# list expectation stores
"""
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_command">
great_expectations store list
# </snippet>
"""

# Override without snippet tag
list_expectation_stores_command = """
great_expectations store list
"""

result = subprocess.run(
    list_expectation_stores_command.strip().split(),
    check=True,
    stdout=subprocess.PIPE,
)
stdout = result.stdout.decode("utf-8")

list_expectation_stores_output = """
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_output">
  - name: expectations_GCS_store
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>
# </snippet>
"""

assert "expectations_GCS_store" in list_expectation_stores_output
assert "expectations_GCS_store" in stdout
assert "TupleGCSStoreBackend" in list_expectation_stores_output
assert "TupleGCSStoreBackend" in stdout

# list expectation suites
"""
# <snippet name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command">
great_expectations suite list
# </snippet>
"""

# Override without snippet tag
list_expectation_suites_command = """
great_expectations suite list
"""

result = subprocess.run(
    list_expectation_suites_command.strip().split(),
    check=True,
    stdout=subprocess.PIPE,
)
stdout = result.stdout.decode("utf-8")

list_expectation_suites_output = """
1 Expectation Suite found:
 - my_expectation_suite
"""

assert "1 Expectation Suite found:" in list_expectation_suites_output
assert "1 Expectation Suite found:" in stdout
assert "my_expectation_suite" in list_expectation_suites_output
assert "my_expectation_suite" in stdout

# clean up this bucket for next time
result = subprocess.run(
    "gsutil rm -r gs://test_metadata_store/how_to_configure_an_expectation_store_in_gcs/expectations".split(),
    check=True,
    stderr=subprocess.PIPE,
)
