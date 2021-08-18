import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

CREDENTIAL = os.getenv("AZURE_CREDENTIAL", "")

context = ge.get_context()

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
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetAzureDataConnector",
            "azure_options": {
                "account_url": "<YOUR_ACCOUNT_URL>",
                "credential": "<YOUR_CREDENTIAL>",
            },
            "container": "superconductive-public",
            "name_starts_with": "data/taxi_yellow_trip_data_samples/",
            "assets": {"taxi_data": None},
            "default_regex": {
                "pattern": "data/taxi_yellow_trip_data_samples/yellow_trip_data_sample_(\\d{4})-(\\d{2})\\.csv",
                "group_names": ["year", "month"],
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["execution_engine"]["azure_options"][
    "account_url"
] = "superconductivetests.blob.core.windows.net"
datasource_config["execution_engine"]["azure_options"]["credential"] = CREDENTIAL
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "azure_options"
]["account_url"] = "superconductivetests.blob.core.windows.net"
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "azure_options"
]["credential"] = CREDENTIAL
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "container"
] = "superconductive-public"
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "name_starts_with"
] = "data/taxi_yellow_trip_data_samples/"

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # this can be anything that identifies this data_asset for you
    runtime_parameters={
        "path": "<PATH_TO_YOUR_DATA_HERE>"
    },  # add your Azure path here.
    batch_identifiers={"default_identifier_name": "something_something"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters[
    "path"
] = f"superconductivetests.blob.core.windows.net/superconductive-public/data/taxi_yellow_trip_data_samples/yellow_trip_data_sample_2018-01.csv"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)

print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)

# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_azure_datasource"]
assert set(
    context.get_available_data_asset_names()["my_azure_datasource"][
        "default_configured_data_connector_name"
    ]
) == {"taxi_data"}
