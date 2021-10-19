from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

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
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "<YOUR_MEANINGFUL_IDENTIFIER>"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = "./data/yellow_trip_data_sample_2019-01.csv"

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
assert "<YOUR_MEANINGFUL_NAME>" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_runtime_data_connector_name"
    ]
)
