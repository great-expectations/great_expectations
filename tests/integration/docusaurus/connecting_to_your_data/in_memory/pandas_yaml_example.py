import pandas as pd
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

datasource_yaml = """
name: example_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
"""

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# creating our example Pandas dataframe
df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["a", "b", "c"])

# Here is a RuntimeBatchRequest that takes in our df as batch_data
batch_request = RuntimeBatchRequest(
    datasource_name="example_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df},  # df is your dataframe
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
