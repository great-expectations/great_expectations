import great_expectations as gx
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.util import get_context
import pathlib

folder_path = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "first_3_files",
    ).resolve(strict=True)
)
data_filepath = folder_path + "/yellow_tripdata_sample_2019-01.csv"

yaml = YAMLHandler()

# NOTE: InMemoryStoreBackendDefaults SHOULD NOT BE USED in normal settings. You
# may experience data loss as it persists nothing. It is used here for testing.
# Please refer to docs to learn how to instantiate your DataContext.
store_backend_defaults = InMemoryStoreBackendDefaults()
data_context_config = DataContextConfig(
    store_backend_defaults=store_backend_defaults,
    checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
)
context = get_context(project_config=data_context_config)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py python">
datasource_config = {
    "name": "my_filesystem_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "<YOUR_PATH>",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\\.csv",
            },
        },
    },
}
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "base_directory"
] = folder_path

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py test_yaml_config">
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py add_datasource">
context.add_datasource(**datasource_config)
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py runtime_batch_request">
# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANINGFUL_NAME>",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = data_filepath

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py runtime_batch_request validator">
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request">
# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019-01"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request validator">
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert [ds["name"] for ds in context.list_datasources()] == ["my_filesystem_datasource"]
assert (
    "yellow_tripdata_sample_2019-01"
    in context.get_available_data_asset_names()["my_filesystem_datasource"][
        "default_inferred_data_connector_name"
    ]
)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request directory">
# Here is a RuntimeBatchRequest using a path to a directory
batch_request = RuntimeBatchRequest(
    datasource_name="my_filesystem_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="example_data_asset",
    runtime_parameters={"path": "taxi_data_files"},
    batch_identifiers={"default_identifier_name": 1234567890},
    batch_spec_passthrough={"reader_method": "csv", "reader_options": {"header": True}},
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = folder_path

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py batch_request directory validator">
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)

print(validator.head())
print(validator.active_batch.data.dataframe.count())  # should be 30,000
# </snippet>

# assert that the 3 files in `data/` (each 10k lines) are read in as a single dataframe
assert validator.active_batch.data.dataframe.count() == 30000
