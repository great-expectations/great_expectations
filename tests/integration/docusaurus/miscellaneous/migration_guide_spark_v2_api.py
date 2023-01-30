import os

from ruamel import yaml

import great_expectations as gx
from great_expectations.data_context.util import file_relative_path

context = gx.get_context()

yaml = yaml.YAML(typ="safe")

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.load(f)

actual_datasource = great_expectations_yaml["datasources"]

# expected Datasource
expected_existing_datasource_yaml = r"""
  my_datasource:
    class_name: SparkDFDatasource
    module_name: great_expectations.datasource
    data_asset_type:
      module_name: great_expectations.dataset
      class_name: SparkDFDataset
    batch_kwargs_generators:
      subdir_reader:
        class_name: SubdirReaderBatchKwargsGenerator
        base_directory: ../../../
"""

assert actual_datasource == yaml.load(expected_existing_datasource_yaml)

# Please note this override is only to provide good UX for docs and tests.
updated_configuration = yaml.load(expected_existing_datasource_yaml)
updated_configuration["my_datasource"]["batch_kwargs_generators"]["subdir_reader"][
    "base_directory"
] = "../data/"
context.add_datasource(name="my_datasource", **updated_configuration["my_datasource"])

actual_validation_operators = great_expectations_yaml["validation_operators"]

# expected Validation Operators
expected_existing_validation_operators_yaml = """
  action_list_operator:
    class_name: ActionListValidationOperator
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
"""
assert actual_validation_operators == yaml.load(
    expected_existing_validation_operators_yaml
)

# check that checkpoint contains the right configuration
# parse great_expectations.yml for comparison
checkpoint_yaml_file_path = os.path.join(
    context.root_directory, "checkpoints/test_v2_checkpoint.yml"
)
with open(checkpoint_yaml_file_path) as f:
    actual_checkpoint_yaml = yaml.load(f)

expected_checkpoint_yaml = """
name: test_v2_checkpoint
config_version:
module_name: great_expectations.checkpoint
class_name: LegacyCheckpoint
validation_operator_name: action_list_operator
batches:
  - batch_kwargs:
      path: ../../data/Titanic.csv
      datasource: my_datasource
      data_asset_name: Titanic.csv
      reader_options:
        header: True
    expectation_suite_names:
      - Titanic.profiled
"""

assert actual_checkpoint_yaml == yaml.load(expected_checkpoint_yaml)

# override for integration tests
updated_configuration = actual_checkpoint_yaml
updated_configuration["batches"][0]["batch_kwargs"]["path"] = file_relative_path(
    __file__, "data/Titanic.csv"
)

# run checkpoint
context.add_checkpoint(**updated_configuration)
results = context.run_checkpoint(checkpoint_name="test_v2_checkpoint")

assert results["success"] is True
