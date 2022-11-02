import os

from ruamel import yaml

import great_expectations as ge

CONNECTION_STRING = "postgresql+psycopg2://postgres:@localhost/test_ci"

# This utility is not for general use. It is only to support testing.
from tests.test_utils import load_data_into_test_database

load_data_into_test_database(
    table_name="titanic",
    csv_path="./data/Titanic.csv",
    connection_string=CONNECTION_STRING,
    load_full_dataset=True,
)

context = ge.get_context()

# parse great_expectations.yml for comparison
great_expectations_yaml_file_path = os.path.join(
    context.root_directory, "great_expectations.yml"
)
with open(great_expectations_yaml_file_path) as f:
    great_expectations_yaml = yaml.safe_load(f)

actual_datasource = great_expectations_yaml["datasources"]

# expected Datasource
expected_existing_datasource_yaml = r"""
  my_postgres_datasource:
    class_name: SqlAlchemyDatasource
    module_name: great_expectations.datasource
    data_asset_type:
      module_name: great_expectations.dataset
      class_name: SqlAlchemyDataset
    connection_string: postgresql+psycopg2://postgres:@localhost/test_ci
"""

assert actual_datasource == yaml.safe_load(expected_existing_datasource_yaml)

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
assert actual_validation_operators == yaml.safe_load(
    expected_existing_validation_operators_yaml
)

# check that checkpoint contains the right configuration
# parse great_expectations.yml for comparison
checkpoint_yaml_file_path = os.path.join(
    context.root_directory, "checkpoints/test_v2_checkpoint.yml"
)
with open(checkpoint_yaml_file_path) as f:
    actual_checkpoint_yaml = yaml.safe_load(f)

expected_checkpoint_yaml = """
name: test_v2_checkpoint
config_version:
module_name: great_expectations.checkpoint
class_name: LegacyCheckpoint
validation_operator_name: action_list_operator
batches:
  - batch_kwargs:
      query: SELECT * from public.titanic
      datasource: my_postgres_datasource
    expectation_suite_names:
      - Titanic.profiled
"""

assert actual_checkpoint_yaml == yaml.safe_load(expected_checkpoint_yaml)

# run checkpoint
results = context.run_checkpoint(checkpoint_name="test_v2_checkpoint")
assert results["success"] is True
