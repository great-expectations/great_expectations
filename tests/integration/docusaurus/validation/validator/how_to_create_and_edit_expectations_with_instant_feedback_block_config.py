# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py imports">
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

# </snippet>

from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()

# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py get_context">
context = gx.get_context()
# </snippet>

datasource_yaml = r"""
name: my_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  my_configured_asset_data_connector:
    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: ./data
    assets:
      my_data_asset:
        pattern: yellow_tripdata_sample_2019-01.csv
"""
context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))
context.add_or_update_expectation_suite(expectation_suite_name="my_expectation_suite")

# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py build_batch_request">
batch_request = BatchRequest(
    datasource_name="my_datasource",
    data_connector_name="my_configured_asset_data_connector",
    data_asset_name="my_data_asset",
)
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py get_validator_and_inspect_data">
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)
validator.head()
# </snippet>

# this snippet is only for users who are not using a jupyter notebook
# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py inspect_data_no_jupyter">
print(validator.head())
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py interactive_validation">
validator.expect_column_values_to_not_be_null(column="vendor_id")
# </snippet>

# this snippet is only for users who are not using a jupyter notebook
# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py interactive_validation_no_jupyter">
expectation_validation_result = validator.expect_column_values_to_not_be_null(
    column="vendor_id"
)
print(expectation_validation_result)
# </snippet>

# <snippet name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_block_config.py save_expectation_suite">
validator.save_expectation_suite(discard_failed_expectations=False)
# </snippet>
