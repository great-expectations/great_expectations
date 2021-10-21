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
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: <MY DIRECTORY>
    default_regex:
      group_names:
        - data_asset_name
      pattern: (.*)\.csv
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<MY DIRECTORY>", "../data/single_directory_one_data_asset/"
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
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "<MY DIRECTORY>",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\.csv",
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "base_directory"
] = "../data/single_directory_one_data_asset/"

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

assert test_yaml == test_python

context.add_datasource(**datasource_config)

assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_tripdata_2019-01" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)

# YAML
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: <MY DIRECTORY>
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)_(\d{4})-(\d{2})\.csv
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<MY DIRECTORY>", "../data/single_directory_one_data_asset/"
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
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "<MY DIRECTORY>",
            "default_regex": {
                "group_names": ["data_asset_name", "year", "month"],
                "pattern": "(.*)_(\d{4})-(\d{2})\.csv",
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "base_directory"
] = "../data/single_directory_one_data_asset/"

test_python = context.test_yaml_config(
    yaml.dump(datasource_config), return_mode="report_object"
)

assert test_yaml == test_python

context.add_datasource(**datasource_config)

assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_tripdata" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)

# YAML
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetS3DataConnector
    bucket: <MY S3 BUCKET>
    prefix: <MY S3 BUCKET PREFIX>
    default_regex:
      group_names:
        - data_asset_name
        - year
        - month
      pattern: (.*)_(\d{4})-(\d{2})\.csv
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<MY S3 BUCKET>", "superconductive-public"
)
datasource_yaml = datasource_yaml.replace(
    "<MY S3 BUCKET PREFIX>", "data/taxi_yellow_trip_data_samples/"
)

# TODO: Uncomment once S3 testing in Azure Pipelines is re-enabled
# test_yaml = context.test_yaml_config(datasource_yaml, return_mode="report_object")

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
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "bucket": "<MY S3 BUCKET>",
            "prefix": "<MY S3 BUCKET PREFIX>",
            "default_regex": {
                "group_names": ["data_asset_name", "year", "month"],
                "pattern": "(.*)_(\d{4})-(\d{2})\.csv",
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the code above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "bucket"
] = "superconductive-public"
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "prefix"
] = "data/taxi_yellow_trip_data_samples/"

# TODO: Uncomment once S3 testing in Azure Pipelines is re-enabled
# test_python = context.test_yaml_config(
#     yaml.dump(datasource_config), return_mode="report_object"
# )
#
# assert test_yaml == test_python
#
# context.add_datasource(**datasource_config)
#
# assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
# assert "yellow_tripdata" in set(
#     context.get_available_data_asset_names()["taxi_datasource"][
#         "default_inferred_data_connector_name"
#     ]
# )
