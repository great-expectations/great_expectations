from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest

context = ge.get_context()

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
            "base_directory": "my_directory/",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\.csv",
            },
        },
    },
}

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "base_directory"
] = "../data/"

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# Here is a BatchRequest using a path to a single CSV file
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_trip_data_sample_2019-01"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
assert "yellow_trip_data_sample_2019-01" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)
