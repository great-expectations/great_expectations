import glob
import os
import subprocess

from ruamel import yaml

import great_expectations as ge

context = ge.get_context()

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(
        "Environment Variable GE_TEST_GCP_PROJECT is required to run GCS integration tests"
    )


datasource_config = r"""
name: my_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: ../data/
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)\.csv
"""
datasource = context.add_datasource(**yaml.safe_load(datasource_config))

expectation_suite_name = "my_expectation_suite"
context.create_expectation_suite(expectation_suite_name=expectation_suite_name)

checkpoint_name = "my_checkpoint"
checkpoint_config = f"""
name: {checkpoint_name}
config_version: 1
class_name: SimpleCheckpoint
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: yellow_tripdata_sample_2019-01
    expectation_suite_name: {expectation_suite_name}
"""
context.add_checkpoint(**yaml.safe_load(checkpoint_config))

# run the checkpoint twice to create two validations
context.run_checkpoint(checkpoint_name=checkpoint_name)
context.run_checkpoint(checkpoint_name=checkpoint_name)

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)

stores = great_expectations_yaml["stores"]
pop_stores = ["checkpoint_store", "evaluation_parameter_store", "expectations_store"]
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
] = gcp_project
configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
    "bucket"
] = "test_metadata_store"
configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
    "prefix"
] = "how_to_configure_a_validation_result_store_in_gcs/validations"

try:
    # clean up validation store from last time if there was a failure mid-script
    delete_validation_store_files = (
        f"gsutil -m rm gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
        + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}/**"
    )
    result = subprocess.run(
        delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
    )
    stderr = result.stderr.decode("utf-8")
    assert "Operation completed" in stderr
except Exception as e:
    pass

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

# try gsutil cp command
copy_validation_command = """
gsutil cp uncommitted/validations/my_expectation_suite/validation_1.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_1.json
gsutil cp uncommitted/validations/my_expectation_suite/validation_2.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_2.json
"""

validation_files = glob.glob(
    f"{context.root_directory}/uncommitted/validations/my_expectation_suite/__none__/*/*.json"
)
copy_validation_command = copy_validation_command.replace(
    "uncommitted/validations/my_expectation_suite/validation_1.json",
    validation_files[0],
)
copy_validation_command = copy_validation_command.replace(
    "uncommitted/validations/my_expectation_suite/validation_2.json",
    validation_files[1],
)
copy_validation_command = copy_validation_command.replace(
    "<YOUR GCS BUCKET NAME>",
    configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
        "bucket"
    ],
)
copy_validation_command = copy_validation_command.replace(
    "<YOUR GCS PREFIX NAME>/validation_1.json",
    configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
        "prefix"
    ]
    + "/validation_1.json",
)
copy_validation_command = copy_validation_command.replace(
    "<YOUR GCS PREFIX NAME>/validation_2.json",
    configured_validations_store["stores"]["validations_GCS_store"]["store_backend"][
        "prefix"
    ]
    + "/validation_2.json",
)

# split two commands to be run one at a time
split_commands = copy_validation_command.strip().split("gsutil")[1:]
split_commands[0] = "gsutil " + split_commands[0].strip()
split_commands[1] = "gsutil " + split_commands[1].strip()

result = subprocess.run(
    split_commands[0], check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")

assert "Operation completed over 1 objects" in stderr

result = subprocess.run(
    split_commands[1], check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")

assert "Operation completed over 1 objects" in stderr

copy_validation_output = """
Operation completed over 2 objects
"""

# list validation stores
list_validation_stores_command = """
great_expectations store list
"""

result = subprocess.run(
    list_validation_stores_command.strip().split(),
    check=True,
    stdout=subprocess.PIPE,
)
stdout = result.stdout.decode("utf-8")

list_validation_stores_output = """
  - name: validations_GCS_store
    class_name: ValidationsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>
"""

assert "validations_GCS_store" in list_validation_stores_output
assert "validations_GCS_store" in stdout
assert "TupleGCSStoreBackend" in list_validation_stores_output
assert "TupleGCSStoreBackend" in stdout

# delete the validations in the GCS store
delete_validation_store_files = (
    f"gsutil -m rm gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}/*.json"
)
result = subprocess.run(
    delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")
assert "Operation completed over 2 objects" in stderr

list_validation_store_files = (
    f"gsutil ls gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}"
)
result = subprocess.run(
    list_validation_store_files, check=True, stdout=subprocess.PIPE, shell=True
)
stdout = result.stdout.decode("utf-8")
assert (
    "gs://test_metadata_store/how_to_configure_a_validation_result_store_in_gcs/validations/my_expectation_suite/"
    not in stdout
)

# get the updated context and run a checkpoint to ensure validation store is updated
context = ge.get_context()
validation_result = context.run_checkpoint(checkpoint_name=checkpoint_name)
assert validation_result["success"] == True
list_validation_store_files = (
    f"gsutil ls gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}"
)
result = subprocess.run(
    list_validation_store_files, check=True, stdout=subprocess.PIPE, shell=True
)
stdout = result.stdout.decode("utf-8")
assert (
    "gs://test_metadata_store/how_to_configure_a_validation_result_store_in_gcs/validations/my_expectation_suite/"
    in stdout
)

# clean up validation store for next time
delete_validation_store_files = (
    f"gsutil -m rm gs://{configured_validations_store['stores']['validations_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validations_store['stores']['validations_GCS_store']['store_backend']['prefix']}/**"
)
result = subprocess.run(
    delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")
assert "Operation completed over 2 objects" in stderr
