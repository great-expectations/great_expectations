import os
from typing import List

import great_expectations as gx
from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

CREDENTIAL = os.getenv("AZURE_ACCESS_KEY", "")

datasource_config = {
    "name": "my_azure_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",
        "azure_options": {
            "account_url": "<YOUR_ACCOUNT_URL>",
            "credential": "<YOUR_CREDENTIAL>",
        },
    },
    "data_connectors": {
        "configured_data_connector_name": {
            "class_name": "ConfiguredAssetAzureDataConnector",
            "azure_options": {
                "account_url": "<YOUR_ACCOUNT_URL>",
                "credential": "<YOUR_CREDENTIAL>",
            },
            "container": "<YOUR_CONTAINER>",
            "name_starts_with": "<CONTAINER_PATH_TO_DATA>",
            "assets": {"taxi_data": None},
            "default_regex": {
                "pattern": "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_(\\d{4})-(\\d{2})\\.csv",
                "group_names": ["year", "month"],
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["azure_options"][
    "account_url"
] = "superconductivetesting.blob.core.windows.net"
datasource_config["execution_engine"]["azure_options"]["credential"] = CREDENTIAL
datasource_config["data_connectors"]["configured_data_connector_name"]["azure_options"][
    "account_url"
] = "superconductivetesting.blob.core.windows.net"
datasource_config["data_connectors"]["configured_data_connector_name"]["azure_options"][
    "credential"
] = CREDENTIAL
datasource_config["data_connectors"]["configured_data_connector_name"][
    "container"
] = "superconductive-public"
datasource_config["data_connectors"]["configured_data_connector_name"][
    "name_starts_with"
] = "data/taxi_yellow_tripdata_samples/"

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": True}},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data"

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
        "configured_data_connector_name"
    ]
) == {"taxi_data"}

batch_list: List[Batch] = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 3

batch: Batch = batch_list[0]
assert batch.data.dataframe.count() == 10000
