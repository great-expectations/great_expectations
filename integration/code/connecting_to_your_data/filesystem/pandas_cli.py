import great_expectations as ge
from great_expectations.cli.datasource import (
    check_if_datasource_name_exists,
    sanitize_yaml_and_save_datasource,
)

context = ge.get_context()

# this comes as a default setting
datasource_name = "my_datasource"
base_directory_name = "/Users/work/Development/ge_data/titanic/"

example_yaml = f"""
name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    datasource_name: {datasource_name}
    base_directory: {base_directory_name}
    default_regex:
      group_names: 
        - data_asset_name
      pattern: (.*)

"""
print(example_yaml)

context.test_yaml_config(yaml_config=example_yaml)

sanitize_yaml_and_save_datasource(context, example_yaml, overwrite_existing=False)
context.list_datasources()


# Second test for BatchRequest naming a table
batch_request = ge.core.batch.BatchRequest(
    datasource_name="my_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="Titanic.csv",  # this is the name of the table you want to retrieve
)
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
