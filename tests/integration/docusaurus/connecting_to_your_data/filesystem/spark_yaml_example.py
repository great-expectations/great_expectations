from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

# NOTE: InMemoryStoreBackendDefaults SHOULD NOT BE USED in normal settings. You
# may experience data loss as it persists nothing. It is used here for testing.
# Please refer to docs to learn how to instantiate your DataContext.
store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = BaseDataContext(project_config=data_context_config)

datasource_yaml = r"""
name: my_filesystem_datasource
class_name: Datasource
execution_engine:
    class_name: SparkDFExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: <YOUR_PATH>
        default_regex:
            group_names:
                - data_asset_name
            pattern: (.*)\.csv
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace("<YOUR_PATH>", "data")

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = "data/yellow_tripdata_sample_2019-01.csv"

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
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019-01"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_filesystem_datasource"]
assert (
    "yellow_tripdata_sample_2019-01"
    in context.get_available_data_asset_names()["my_filesystem_datasource"][
        "default_inferred_data_connector_name"
    ]
)


# Here is a RuntimeBatchRequest using a path to a directory
batch_request = RuntimeBatchRequest(
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANINGFUL_NAME>",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "something_something"},
    batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": True}},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = "data/"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)

print(validator.head())
print(validator.active_batch.data.dataframe.count())  # should be 30,000

# assert that the 3 files in `data/` (each 10k lines) are read in as a single dataframe
assert validator.active_batch.data.dataframe.count() == 30000
