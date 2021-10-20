from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest

context = ge.get_context()

# YAML
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_configured_data_connector_name:
    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: my_directory/
    assets:
      taxi:
        pattern: (.*)\.csv
        group_names:
          - month
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "my_directory/", "../data/single_directory_one_data_asset/"
)

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python
datasource_config = {
    "name": "taxi_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": "my_directory/",
            "assets": {
                "taxi": {
                    "pattern": "yellow_tripdata_(.*)\.csv",
                    "group_names": ["month"],
                }
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "base_directory"
] = "../data/single_directory_one_data_asset/"

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

assert test_yaml == test_python

context.add_datasource(**datasource_config)

# Here is a BatchRequest using a path to a single CSV file
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="taxi",
)

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="test_suite",
    batch_identifiers={"month": "2019-02"},
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "taxi" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_configured_data_connector_name"
    ]
)
