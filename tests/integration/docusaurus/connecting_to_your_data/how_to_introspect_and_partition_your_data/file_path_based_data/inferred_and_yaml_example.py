from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest

# TODO: <Alex>ALEX</Alex>
# context = ge.get_context()
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
import os

from great_expectations.data_context.util import file_relative_path

script_path = os.path.dirname(os.path.realpath(__file__))
print(f'\n[ALEX_TEST] SCRIPT_PATH_DIR: {script_path}')
data_path_dir = script_path + "/../../../../../test_sets/taxi_yellow_trip_data_samples/"
print(f'\n[ALEX_TEST] DATA_PATH_DIR: {data_path_dir}')
wd = os.getcwd()
print(f'\n[ALEX_TEST] CURRENT_WORKING_DIRECTORY: {wd}')
# tests/test_sets/taxi_yellow_trip_data_samples
data_dir_path = file_relative_path(__file__, os.path.join("..", "..", "..", "..", "..", "test_sets", "taxi_yellow_trip_data_samples"))
print(f'\n[ALEX_TEST] DATA_DIRECTORY_PATH: {data_dir_path}')
from tests.expectations.test_expectation_arguments import build_in_memory_runtime_context
context = build_in_memory_runtime_context()
# TODO: <Alex>ALEX</Alex>


datasource_yaml = f"""
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    configured_data_connector_name:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: <PATH_TO_YOUR_DATA_HERE>
        glob_directive: "*.csv"
        default_regex:
          pattern: (.*)
          group_names:
            - data_asset_name
        assets:
          taxi_data_flat:
            base_directory: first_3_files
            pattern: (yellow_trip_data_sample_.*)\\.csv
            group_names:
              - filename
          taxi_data_year_month:
            base_directory: first_3_files
            pattern: yellow_trip_data_sample_(\\d{{4}})-(\\d{{2}})\\.csv
            group_names:
              - year
              - month
          # TODO: <Alex>ALEX</Alex>
          # taxi_data_year_month_single_passenger:
          #   base_directory: first_3_files
          #   pattern: yellow_trip_data_sample_(\\d{{4}})-(\\d{{2}})\\.csv
          #   group_names:
          #     - year
          #     - month
          #   splitter_method: _split_on_column_value
          #   splitter_kwargs:
          #     column_name: passenger_count
          #     batch_identifiers:
          #       passenger_count: 1
          # TODO: <Alex>ALEX</Alex>
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: <PATH_TO_YOUR_DATA_HERE>
        glob_directive: "*.csv"
        default_regex:
          pattern: (.*)
          group_names:
            - data_asset_name
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
datasource_yaml = datasource_yaml.replace(
    # TODO: <Alex>ALEX</Alex>
    # "<PATH_TO_YOUR_DATA_HERE>", "../../../../../test_sets/taxi_yellow_trip_data_samples/"
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    # "<PATH_TO_YOUR_DATA_HERE>", "./tests/test_sets/taxi_yellow_trip_data_samples/" #data_dir_path
    "<PATH_TO_YOUR_DATA_HERE>", data_dir_path
    # TODO: <Alex>ALEX</Alex>
)

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))
print(f'\n[ALEX_TEST] WOUTPUT-ASSETS: {context.get_available_data_asset_names()}')
print(f'\n[ALEX_TEST] WOUTPUT-NUM_ASSETS: {len(context.get_available_data_asset_names())}')
print(f'\n[ALEX_TEST] WOUTPUT-ASSETS-TAXI: {context.datasources["taxi_datasource"].get_available_data_asset_names()}')
print(f'\n[ALEX_TEST] WOUTPUT-NUM_ASSETS-TAXI: {len(context.datasources["taxi_datasource"].get_available_data_asset_names()["default_inferred_data_connector_name"])}')

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)

# Here is a BatchRequest naming an inferred data_asset.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_trip_data_sample_2019-01.csv"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# Here is a BatchRequest naming a configured data_asset representing an un-partitioned (flat) filename structure.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_flat"
print(f'\n[ALEX_TEST] BATCH_REQUEST:\n{batch_request} ; TYPE: {str(type(batch_request))}')
batch_list = context.get_batch_list(
    batch_request=batch_request
)
assert len(batch_list) == 3
print(f'\n[ALEX_TEST] NUM_BATCHES:\n{len(batch_list)}')

# Here is a BatchRequest naming a configured data_asset representing a filename structure partitioned by year and month.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    data_connector_query={
        "batch_filter_parameters": {
            "month": "01",
        }
    },
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_year_month"
print(f'\n[ALEX_TEST] BATCH_REQUEST:\n{batch_request} ; TYPE: {str(type(batch_request))}')
batch_list = context.get_batch_list(
    batch_request=batch_request
)
assert len(batch_list) == 1
print(f'\n[ALEX_TEST] NUM_BATCHES:\n{len(batch_list)}')

assert batch_list[0].data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a configured data_asset representing a filename structure partitioned by year and month.
# In addition, the resulting batch is split according to "passenger_count" column to retrieve single passenger records.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    data_connector_query={
        "batch_filter_parameters": {
            "month": "01",
        }
    },
    # TODO: <Alex>ALEX</Alex>
    batch_spec_passthrough={
        "splitter_method": "_split_on_column_value",
        "splitter_kwargs": {
            "column_name": "passenger_count",
            "batch_identifiers": {"passenger_count": 2},
        },
    },
    # TODO: <Alex>ALEX</Alex>
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
# TODO: <Alex>ALEX</Alex>
# batch_request.data_asset_name = "taxi_data_year_month_single_passenger"
# TODO: <Alex>ALEX</Alex>
# TODO: <Alex>ALEX</Alex>
batch_request.data_asset_name = "taxi_data_year_month"
# TODO: <Alex>ALEX</Alex>
print(f'\n[ALEX_TEST] BATCH_REQUEST:\n{batch_request} ; TYPE: {str(type(batch_request))}')
batch_list = context.get_batch_list(
    batch_request=batch_request
)
assert len(batch_list) == 1
print(f'\n[ALEX_TEST] NUM_BATCHES:\n{len(batch_list)}')

# TODO: <Alex>ALEX</Alex>
# assert batch_list[0].data.dataframe.shape[0] == 10000
# TODO: <Alex>ALEX</Alex>
print(f'\n[ALEX_TEST] NUM_ROWS: {batch_list[0].data.dataframe.shape[0]}')
# TODO: <Alex>ALEX</Alex>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
# TODO: <Alex>ALEX</Alex>
# assert [ds["name"] for ds in context.list_datasources()] == ["taxi_datasource"]
# TODO: <Alex>ALEX</Alex>
assert "taxi_datasource" in [ds["name"] for ds in context.list_datasources()]
# TODO: <Alex>ALEX</Alex>
assert "yellow_trip_data_sample_2019-01.csv" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)
