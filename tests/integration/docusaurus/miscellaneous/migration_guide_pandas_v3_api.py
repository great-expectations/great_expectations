import os

from ruamel import yaml

import great_expectations as gx

context = gx.get_context()

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)

actual_datasource = great_expectations_yaml["datasources"]

# expected Datasource
expected_existing_datasource_yaml = r"""
  my_datasource:
    module_name: great_expectations.datasource
    class_name: Datasource
    execution_engine:
      module_name: great_expectations.execution_engine
      class_name: PandasExecutionEngine
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: ../../../data/
        module_name: great_expectations.datasource.data_connector
        default_regex:
          group_names:
            - data_asset_name
          pattern: (.*)
      default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name
        module_name: great_expectations.datasource.data_connector
"""

assert actual_datasource == yaml.safe_load(expected_existing_datasource_yaml)

# Please note this override is only to provide good UX for docs and tests.
updated_configuration = yaml.safe_load(expected_existing_datasource_yaml)
updated_configuration["my_datasource"]["data_connectors"][
    "default_inferred_data_connector_name"
]["base_directory"] = "../data/"
context.add_datasource(name="my_datasource", **updated_configuration["my_datasource"])

# check that checkpoint contains the right configuration
# parse great_expectations.yml for comparison
checkpoint_yaml_file_path = os.path.join(
    context.root_directory, "checkpoints/test_v3_checkpoint.yml"
)
with open(checkpoint_yaml_file_path) as f:
    actual_checkpoint_yaml = yaml.safe_load(f)

expected_checkpoint_yaml = """
name: test_v3_checkpoint
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name:
batch_request:
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
evaluation_parameters: {}
runtime_configuration: {}
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: Titanic.csv
      data_connector_query:
        index: -1
    expectation_suite_name: Titanic.profiled
profilers: []
ge_cloud_id:
expectation_suite_ge_cloud_id:
"""

assert actual_checkpoint_yaml == yaml.safe_load(expected_checkpoint_yaml)

# run checkpoint
context.add_checkpoint(**actual_checkpoint_yaml)
results = context.run_checkpoint(checkpoint_name="test_v3_checkpoint")

assert results["success"] is True
