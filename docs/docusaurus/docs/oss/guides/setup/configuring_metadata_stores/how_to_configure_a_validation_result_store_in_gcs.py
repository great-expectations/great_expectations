import glob
import os
import pathlib
import subprocess

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)

yaml = YAMLHandler()
context = gx.get_context()

# NOTE: The following code is only for testing and depends on an environment
# variable to set the gcp_project. You can replace the value with your own
# GCP project information
gcp_project = os.environ.get("GE_TEST_GCP_PROJECT")
if not gcp_project:
    raise ValueError(  # noqa: TRY003
        "Environment Variable GE_TEST_GCP_PROJECT is required to run GCS integration tests"
    )

datasource = context.data_sources.add_pandas_filesystem(
    name="my_datasource",
    base_directory="../data/",  # type: ignore
)
asset = datasource.add_csv_asset("yellow_tripdata_samples")

expectation_suite_name = "my_expectation_suite"
suite = context.suites.add(ExpectationSuite(name=expectation_suite_name))

checkpoint_name = "my_checkpoint"
checkpoint = context.checkpoints.add(
    Checkpoint(
        name=checkpoint_name,
        validation_definitions=[
            context.validation_definitions.add(
                ValidationDefinition(
                    name="my_validation_definition",
                    expectation_suite=suite,
                    data=asset.add_batch_definition_path(
                        name="2019-01", path="yellow_tripdata_sample_2019-01.csv"
                    ),
                )
            )
        ],
    )
)

# run the checkpoint twice to create two validations
checkpoint.run()
checkpoint.run()

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = pathlib.Path(
    context.root_directory, FileDataContext.GX_YML
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

stores = great_expectations_yaml["stores"]
pop_stores = ["checkpoint_store", "expectations_store"]
for store in pop_stores:
    stores.pop(store)

actual_existing_validation_results_store = {}
actual_existing_validation_results_store["stores"] = stores
actual_existing_validation_results_store["validation_results_store_name"] = (
    great_expectations_yaml["validation_results_store_name"]
)

expected_existing_validation_results_store_yaml = """
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py expected_existing_validation_results_store_yaml">
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

configured_validation_results_store_yaml = """
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py configured_validation_results_store_yaml">
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
]["prefix"] = "how_to_configure_a_validation_result_store_in_gcs/validations"

try:
    # clean up validation store from last time if there was a failure mid-script
    delete_validation_store_files = (
        f"gsutil -m rm gs://{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['bucket']}"
        + f"/{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['prefix']}/**"
    )
    result = subprocess.run(
        delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
    )
    stderr = result.stderr.decode("utf-8")
    assert "Operation completed" in stderr
except Exception:
    pass

# add and set the new validation store
context.add_store(
    store_name=configured_validation_results_store["validation_results_store_name"],
    store_config=configured_validation_results_store["stores"][
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

"""
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command">
gsutil cp uncommitted/validations/my_expectation_suite/validation_1.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_1.json
gsutil cp uncommitted/validations/my_expectation_suite/validation_2.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_2.json
# </snippet>
"""

# Override without snippet tag
# try gsutil cp command
copy_validation_command = """
gsutil cp uncommitted/validations/my_expectation_suite/validation_1.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_1.json
gsutil cp uncommitted/validations/my_expectation_suite/validation_2.json gs://<YOUR GCS BUCKET NAME>/<YOUR GCS PREFIX NAME>/validation_2.json
"""

validation_files = glob.glob(  # noqa: PTH207 # can use Path.glob
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
    configured_validation_results_store["stores"]["validation_results_GCS_store"][
        "store_backend"
    ]["bucket"],
)
copy_validation_command = copy_validation_command.replace(
    "<YOUR GCS PREFIX NAME>/validation_1.json",
    configured_validation_results_store["stores"]["validation_results_GCS_store"][
        "store_backend"
    ]["prefix"]
    + "/validation_1.json",
)
copy_validation_command = copy_validation_command.replace(
    "<YOUR GCS PREFIX NAME>/validation_2.json",
    configured_validation_results_store["stores"]["validation_results_GCS_store"][
        "store_backend"
    ]["prefix"]
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
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output">
Operation completed over 2 objects
# </snippet>
"""

"""
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py list_validation_stores_command">
great_expectations store list
# </snippet>
"""

# Override without snippet tag
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
# <snippet name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py list_validation_stores_output">
  - name: validation_results_GCS_store
    class_name: ValidationResultsStore
    store_backend:
      class_name: TupleGCSStoreBackend
      project: <YOUR GCP PROJECT NAME>
      bucket: <YOUR GCS BUCKET NAME>
      prefix: <YOUR GCS PREFIX NAME>
# </snippet>
"""

assert "validation_results_GCS_store" in list_validation_stores_output
assert "validation_results_GCS_store" in stdout
assert "TupleGCSStoreBackend" in list_validation_stores_output
assert "TupleGCSStoreBackend" in stdout

# delete the validations in the GCS store
delete_validation_store_files = (
    f"gsutil -m rm gs://{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['prefix']}/*.json"
)
result = subprocess.run(
    delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")
assert "Operation completed over 2 objects" in stderr

list_validation_store_files = (
    f"gsutil ls gs://{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['prefix']}"
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
context = gx.get_context()
checkpoint = context.get_legacy_checkpoint(checkpoint_name)
validation_result = checkpoint.run()
assert validation_result["success"] is True
list_validation_store_files = (
    f"gsutil ls gs://{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['prefix']}"
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
    f"gsutil -m rm gs://{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['bucket']}"
    + f"/{configured_validation_results_store['stores']['validation_results_GCS_store']['store_backend']['prefix']}/**"
)
result = subprocess.run(
    delete_validation_store_files, check=True, stderr=subprocess.PIPE, shell=True
)
stderr = result.stderr.decode("utf-8")
assert "Operation completed over 2 objects" in stderr
