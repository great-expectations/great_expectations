from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.validator.validation_graph import MetricConfiguration

context = ge.get_context()

docs_test_matrix = [
    {
        "name": "how_to_get_a_batch_of_data_from_a_configured_datasource",
        "user_flow_script": "tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py",
        "data_context_dir": "tests/integration/fixtures/yellow_trip_data_pandas_fixture/great_expectations",
        "data_dir": "tests/test_sets/taxi_yellow_trip_data_samples",
    },
]

# Please note the naming of this datasource is only to provide good UX for docs and tests.
datasource_yaml = fr"""
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

# Here is a BatchRequest for all batches associated with the specified DataAsset
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

# Alternatively you can use the previously created batch_request
batch_list_all_b = context.get_batch_list(batch_request=batch_request)
assert len(batch_list_all_b) == 36

# Or use a query to filter the batch_list
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
batch_list_all_limit_10 = context.get_batch_list(batch_request=batch_request, limit=10)
# TODO: Fix limit param and un-comment out this assertion
# assert len(batch_list_all_limit_10) == 10


# TODO: CLEAN UP THE BELOW vvvv

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=last_index_batch_request, expectation_suite_name="test_suite"
)
print(validator.head())


# NOTE: The following assertions are only for testing and can be ignored by users.
row_count = validator.get_metric(
    MetricConfiguration(
        "table.row_count",
        # metric_domain_kwargs={"batch_id": validator.active_batch_id}
        metric_domain_kwargs={"batch_id": batch_list_last_index_batch_request[0].id},
    )
)
# TODO: uncomment and clean up when limit is fixed
# assert row_count == limit, f"row_count: {row_count}"

# NOTE: The following assertions are only for testing and can be ignored by users.
assert validator.active_batch_id == batch_list_last_index_batch_request[0].id

assert (
    validator.active_batch.batch_definition.batch_identifiers["name"]
    == "yellow_trip_data_sample"
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
    == "yellow_trip_data_sample"
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
