import os
from typing import List

# <snippet>
from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import Batch, BatchRequest

# </snippet>

CREDENTIAL = os.getenv("AZURE_CREDENTIAL", "")

# <snippet>
context = ge.get_context()
# </snippet>

# <snippet>
datasource_yaml = rf"""
name: my_azure_datasource
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine
    azure_options:
        account_url: <YOUR_ACCOUNT_URL> # or `conn_str`
        credential: <YOUR_CREDENTIAL>   # if using a protected container
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_inferred_data_connector_name:
        class_name: InferredAssetAzureDataConnector
        azure_options:
            account_url: <YOUR_ACCOUNT_URL> # or `conn_str`
            credential: <YOUR_CREDENTIAL>   # if using a protected container
        container: <YOUR_AZURE_CONTAINER_HERE>
        name_starts_with: <CONTAINER_PATH_TO_DATA>
        default_regex:
            pattern: (.*)\.csv
            group_names:
                - data_asset_name
"""
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    "<YOUR_AZURE_CONTAINER_HERE>", "superconductive-public"
)
datasource_yaml = datasource_yaml.replace(
    "<CONTAINER_PATH_TO_DATA>", "data/taxi_yellow_tripdata_samples/"
)
datasource_yaml = datasource_yaml.replace(
    "<YOUR_ACCOUNT_URL>", "superconductivetesting.blob.core.windows.net"
)
datasource_yaml = datasource_yaml.replace("<YOUR_CREDENTIAL>", CREDENTIAL)

# <snippet>
context.test_yaml_config(datasource_yaml)
# </snippet>

# <snippet>
context.add_datasource(**yaml.load(datasource_yaml))
# </snippet>

# Here is a BatchRequest naming a data_asset
# <snippet>
batch_request = BatchRequest(
    datasource_name="my_azure_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = (
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01"
)

# <snippet>
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
# </snippet>
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
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
