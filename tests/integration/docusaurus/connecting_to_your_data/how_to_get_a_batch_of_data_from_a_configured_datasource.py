from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.validator.metric_configuration import MetricConfiguration

context = ge.get_context()

# Please note the naming of this datasource is only to provide good UX for docs and tests.
datasource_yaml = rf"""
name: insert_your_datasource_name_here
module_name: great_expectations.datasource
class_name: Datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
  insert_your_data_connector_name_here:
    base_directory: ../data/
    glob_directive: '*.csv'
    class_name: ConfiguredAssetFilesystemDataConnector
    assets:
      insert_your_data_asset_name_here:
        base_directory: ./
        group_names:
          - name
          - param_1_from_your_data_connector_eg_year
          - param_2_from_your_data_connector_eg_month
        module_name: great_expectations.datasource.data_connector.asset
        class_name: Asset
        pattern: (.+)_(\d.*)-(\d.*)\.csv
    module_name: great_expectations.datasource.data_connector
"""

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))

# Here is an example BatchRequest for all batches associated with the specified DataAsset
batch_request = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(context.get_batch_list(batch_request=batch_request)) == 36

# This BatchRequest adds a query to retrieve only the twelve batches from 2020
data_connector_query_2020 = {
    "batch_filter_parameters": {"param_1_from_your_data_connector_eg_year": "2020"}
}
batch_request_2020 = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    data_connector_query=data_connector_query_2020,
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(context.get_batch_list(batch_request=batch_request_2020)) == 12

# This BatchRequest adds a query and limit to retrieve only the first 5 batches from 2020
data_connector_query_2020 = {
    "batch_filter_parameters": {"param_1_from_your_data_connector_eg_year": "2020"}
}
batch_request_2020 = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    data_connector_query=data_connector_query_2020,
    limit=5,
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(context.get_batch_list(batch_request=batch_request_2020)) == 5

# Here is an example `data_connector_query` filtering based on parameters from `group_names`
# previously defined in a regex pattern in your Data Connector:
data_connector_query_202001 = {
    "batch_filter_parameters": {
        "param_1_from_your_data_connector_eg_year": "2020",
        "param_2_from_your_data_connector_eg_month": "01",
    }
}
# This BatchRequest will use the above filter to retrieve only the batch from Jan 2020
batch_request_202001 = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    data_connector_query=data_connector_query_202001,
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(context.get_batch_list(batch_request=batch_request_202001)) == 1


# Here is an example `data_connector_query` filtering based on an `index` which can be
# any valid python slice. The example here is retrieving the latest batch using `-1`:
data_connector_query_last_index = {
    "index": -1,
}
last_index_batch_request = BatchRequest(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    data_connector_query=data_connector_query_last_index,
)
# NOTE: The following assertion is only for testing and can be ignored by users.
assert len(context.get_batch_list(batch_request=last_index_batch_request)) == 1


# List all Batches associated with the DataAsset
batch_list_all_a = context.get_batch_list(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
)
assert len(batch_list_all_a) == 36

# Alternatively you can use the previously created batch_request to achieve the same thing
batch_list_all_b = context.get_batch_list(batch_request=batch_request)
assert len(batch_list_all_b) == 36

# You can use a query to filter the batch_list
batch_list_202001_query = context.get_batch_list(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    data_connector_query=data_connector_query_202001,
)
assert len(batch_list_202001_query) == 1

# Which is equivalent to the batch_request
batch_list_last_index_batch_request = context.get_batch_list(
    batch_request=last_index_batch_request
)
assert len(batch_list_last_index_batch_request) == 1

# Or limit to a specific number of batches
batch_list_all_limit_10 = context.get_batch_list(
    datasource_name="insert_your_datasource_name_here",
    data_connector_name="insert_your_data_connector_name_here",
    data_asset_name="insert_your_data_asset_name_here",
    limit=10,
)
assert len(batch_list_all_limit_10) == 10

# Now we can review a sample of data using a Validator

# First create an expectation suite to use with our validator
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
# Now create our validator
validator = context.get_validator(
    batch_request=last_index_batch_request, expectation_suite_name="test_suite"
)
# View the first few lines of the loaded Batch
print(validator.head())


# NOTE: The following assertions are only for testing and can be ignored by users.
assert validator.active_batch_id == batch_list_last_index_batch_request[0].id
assert len(validator.batches) == 1

row_count = validator.get_metric(
    MetricConfiguration(
        "table.row_count", metric_domain_kwargs={"batch_id": validator.active_batch_id}
    )
)
assert row_count == 10000

assert (
    validator.active_batch.batch_definition.batch_identifiers["name"]
    == "yellow_tripdata_sample"
)
assert (
    validator.active_batch.batch_definition.batch_identifiers[
        "param_1_from_your_data_connector_eg_year"
    ]
    == "2020"
)
assert (
    validator.active_batch.batch_definition.batch_identifiers[
        "param_2_from_your_data_connector_eg_month"
    ]
    == "12"
)

assert (
    batch_list_last_index_batch_request[0].batch_definition.batch_identifiers["name"]
    == "yellow_tripdata_sample"
)
assert (
    batch_list_last_index_batch_request[0].batch_definition.batch_identifiers[
        "param_1_from_your_data_connector_eg_year"
    ]
    == "2020"
)
assert (
    batch_list_last_index_batch_request[0].batch_definition.batch_identifiers[
        "param_2_from_your_data_connector_eg_month"
    ]
    == "12"
)

assert isinstance(validator, ge.validator.validator.Validator)
assert "insert_your_datasource_name_here" in [
    ds["name"] for ds in context.list_datasources()
]
assert "insert_your_data_asset_name_here" in set(
    context.get_available_data_asset_names()["insert_your_datasource_name_here"][
        "insert_your_data_connector_name_here"
    ]
)
