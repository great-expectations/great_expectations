import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

# YAML
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py datasource_yaml">
datasource_yaml = r"""
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: <MY DIRECTORY>/
    glob_directive: "*/*.csv"
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)/.*(\d{4})-(\d{2})\.csv
"""
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<MY DIRECTORY>/", "../data/nested_directories_data_asset/"
)

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py datasource_config">
datasource_config = {
    "name": "taxi_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "<MY DIRECTORY>/",
            "glob_directive": "*/*.csv",
            "default_regex": {
                "group_names": [
                    "data_asset_name",
                    "year",
                    "month",
                ],
                "pattern": r"(.*)/.*(\d{4})-(\d{2})\.csv",
            },
        },
    },
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "base_directory"
] = "../data/nested_directories_data_asset/"

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python

context.add_datasource(**datasource_config)

assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_tripdata" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)
assert "green_tripdata" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)

# YAML
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py datasource_yaml_2">
datasource_yaml = r"""
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_configured_data_connector_name:
    class_name: ConfiguredAssetFilesystemDataConnector
    base_directory: <MY DIRECTORY>/
    assets:
      yellow_tripdata:
        base_directory: yellow_tripdata/
        pattern: yellow_tripdata_(\d{4})-(\d{2})\.csv
        group_names:
          - year
          - month
      green_tripdata:
        base_directory: green_tripdata/
        pattern: (\d{4})-(\d{2})\.csv
        group_names:
          - year
          - month
"""
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<MY DIRECTORY>/", "../data/nested_directories_data_asset/"
)

test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py datasource_config_2">
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
            "base_directory": "<MY DIRECTORY>/",
            "assets": {
                "yellow_tripdata": {
                    "base_directory": "yellow_tripdata/",
                    "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv",
                    "group_names": ["year", "month"],
                },
                "green_tripdata": {
                    "base_directory": "green_tripdata/",
                    "pattern": r"(\d{4})-(\d{2})\.csv",
                    "group_names": ["year", "month"],
                },
            },
        },
    },
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "base_directory"
] = "../data/nested_directories_data_asset/"

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

# NOTE: The following code is only for testing and can be ignored by users.
assert test_yaml == test_python
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_tripdata" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_configured_data_connector_name"
    ]
)
assert "green_tripdata" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_configured_data_connector_name"
    ]
)
