import os
from typing import List

import great_expectations as gx
from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

CREDENTIAL = os.getenv("AZURE_CREDENTIAL", "")

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py datasource config">
datasource_config = {
    "name": "my_azure_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "PandasExecutionEngine",
        "azure_options": {
            "account_url": "<YOUR_ACCOUNT_URL>",
            "credential": "<YOUR_CREDENTIAL>",
        },
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetAzureDataConnector",
            "azure_options": {
                "account_url": "<YOUR_ACCOUNT_URL>",
                "credential": "<YOUR_CREDENTIAL>",
            },
            "container": "<YOUR_CONTAINER>",
            "name_starts_with": "<CONTAINER_PATH_TO_DATA>",
            "default_regex": {
                "pattern": "(.*)\\.csv",
                "group_names": ["data_asset_name"],
            },
        },
    },
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["azure_options"][
    "account_url"
] = "superconductivetesting.blob.core.windows.net"
datasource_config["execution_engine"]["azure_options"]["credential"] = CREDENTIAL
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "azure_options"
]["account_url"] = "superconductivetesting.blob.core.windows.net"
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "azure_options"
]["credential"] = CREDENTIAL
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "container"
] = "superconductive-public"
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "name_starts_with"
] = "data/taxi_yellow_tripdata_samples/"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py test datasource">
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py add datasource">
context.add_datasource(**datasource_config)
# </snippet>

# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = (
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01"
)

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_azure_datasource"]
assert set(
    context.get_available_data_asset_names()["my_azure_datasource"][
        "default_inferred_data_connector_name"
    ]
) == {
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01",
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02",
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-03",
}

batch_list: List[Batch] = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1

batch: Batch = batch_list[0]
assert batch.data.dataframe.shape[0] == 10000
