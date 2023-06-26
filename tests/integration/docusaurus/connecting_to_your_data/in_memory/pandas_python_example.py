import pandas as pd

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py datasource_config">
datasource_config = {
    "name": "example_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py test_yaml_config">
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/in_memory/pandas_python_example.py add_datasource">
context.add_datasource(**datasource_config)
# </snippet>

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

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
