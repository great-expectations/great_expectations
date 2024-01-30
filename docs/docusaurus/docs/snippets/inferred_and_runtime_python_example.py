from typing import List

import great_expectations as gx
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
context = gx.get_context()

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py datasource_config">
datasource_config = {
    "name": "my_s3_datasource",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetS3DataConnector",
            "bucket": "<YOUR_S3_BUCKET_HERE>",
            "prefix": "<BUCKET_PATH_TO_DATA>",
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
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "bucket"
] = "superconductive-docs-test"
datasource_config["data_connectors"]["default_inferred_data_connector_name"][
    "prefix"
] = "data/taxi_yellow_tripdata_samples/"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py test_yaml_config">
context.test_yaml_config(yaml.dump(datasource_config))
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/cloud/s3/pandas/inferred_and_runtime_python_example.py add_datasource">
context.add_datasource(**datasource_config)
# </snippet>

# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="my_s3_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "<PATH_TO_YOUR_DATA_HERE>"},  # Add your S3 path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters[
    "path"
] = "s3a://superconductive-docs-test/data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv"

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)

batch_list: List[Batch] = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1

batch: Batch = batch_list[0]
assert batch.data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a data_asset
batch_request = BatchRequest(
    datasource_name="my_s3_datasource",
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
assert [ds["name"] for ds in context.list_datasources()] == ["my_s3_datasource"]
assert set(
    context.get_available_data_asset_names()["my_s3_datasource"][
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
