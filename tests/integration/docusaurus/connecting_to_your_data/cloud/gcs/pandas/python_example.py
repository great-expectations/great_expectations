from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

context = ge.get_context()

datasource_config = {
    "name": "my_gcs_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetGCSDataConnector",
            "bucket_or_name": "<YOUR_GCS_BUCKET_HERE>",
            "prefix": "<BUCKET_PATH_TO_DATA>",
            "default_regex": {
                "pattern": "data/taxi_yellow_trip_data_samples/yellow_trip_data_sample_(\\d{{4}})-(\\d{{2}})\\.csv",
                "group_names": ["year", "month"],
            },
        },
    },
}

datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "bucket_or_name"
] = "superconductive-integration-tests"
datasource_config["data_connectors"]["default_configured_data_connector_name"][
    "prefix"
] = "data/taxi_yellow_trip_data_samples"

datasource_yaml = f"""
name: my_gcs_datasource
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_configured_data_connector_name:
        class_name: ConfiguredAssetGCSDataConnector
        bucket_or_name: <YOUR_GCS_BUCKET_HERE>
        prefix: <BUCKET_PATH_TO_DATA>
        assets:
            taxi_data:
        default_regex:
            pattern: data/taxi_yellow_trip_data_samples/yellow_trip_data_sample_(\\d{{4}})-(\\d{{2}})\\.csv
            group_names:
                - year
                - month
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<YOUR_GCS_BUCKET_HERE>", "superconductive-integration-tests"
)
datasource_yaml = datasource_yaml.replace(
    "<BUCKET_PATH_TO_DATA>", "data/taxi_yellow_trip_data_samples/"
)

print(yaml.load(datasource_yaml))

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="my_gcs_datasource",
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
] = f"gs://superconductive-integration-tests/data/taxi_yellow_trip_data_samples/yellow_trip_data_sample_2019-01.csv"

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
    datasource_name="my_gcs_datasource",
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
assert [ds["name"] for ds in context.list_datasources()] == ["my_gcs_datasource"]
assert set(
    context.get_available_data_asset_names()["my_gcs_datasource"][
        "default_configured_data_connector_name"
    ]
) == {"taxi_data"}
