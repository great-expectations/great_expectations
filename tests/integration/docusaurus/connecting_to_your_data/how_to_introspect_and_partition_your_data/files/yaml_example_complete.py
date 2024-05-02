# ruff: noqa: PTH118
import os

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py imports">
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

yaml = YAMLHandler()
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py get_context">
context = gx.get_context()
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py datasource_yaml">
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
module_name: great_expectations.datasource
execution_engine:
  module_name: great_expectations.execution_engine
  class_name: PandasExecutionEngine
data_connectors:
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: <PATH_TO_YOUR_DATA_HERE>
        glob_directive: "*.csv"
        default_regex:
          pattern: (.*)
          group_names:
            - data_asset_name

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
            base_directory: samples_2020
            pattern: (yellow_tripdata_sample_.+)\\.csv
            group_names:
              - filename
          taxi_data_year_month:
            base_directory: samples_2020
            pattern: ([\\w]+)_tripdata_sample_(\\d{4})-(\\d{2})\\.csv
            group_names:
              - name
              - year
              - month
"""
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
data_dir_path = os.path.join("..", "data")

datasource_yaml = datasource_yaml.replace("<PATH_TO_YOUR_DATA_HERE>", data_dir_path)

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py test_yaml_config">
context.test_yaml_config(datasource_yaml)
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py add_datasource">
context.add_datasource(**yaml.load(datasource_yaml))
# </snippet>
available_data_asset_names = context.datasources["taxi_datasource"].get_available_data_asset_names(
    data_connector_names="default_inferred_data_connector_name"
)["default_inferred_data_connector_name"]
assert len(available_data_asset_names) == 36

# Here is a BatchRequest naming an inferred data_asset.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019-01.csv"

context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
validator = context.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")
print(validator.head(n_rows=10))

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1
assert batch_list[0].data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a configured data_asset representing an un-partitioned (flat) filename structure.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_flat"

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 12
assert batch_list[0].data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a configured data_asset representing a filename structure partitioned by year and month.
# This BatchRequest specifies multiple batches, which is useful for dataset exploration.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    data_connector_query={"custom_filter_function": "<YOUR_CUSTOM_FILTER_FUNCTION>"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name and other arguments directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_year_month"
batch_request.data_connector_query["custom_filter_function"] = (
    lambda batch_identifiers: batch_identifiers["name"] == "yellow"
    and 1 < int(batch_identifiers["month"]) < 11
)

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 9
assert batch_list[0].data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a configured data_asset representing a filename structure partitioned by year and month.
# This BatchRequest specifies one batch, which is useful for data analysis.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    data_connector_query={
        "batch_filter_parameters": {
            "<YOUR_BATCH_FILTER_PARAMETER_0_KEY>": "<YOUR_BATCH_FILTER_PARAMETER_0_VALUE>",
            "<YOUR_BATCH_FILTER_PARAMETER_1_KEY>": "<YOUR_BATCH_FILTER_PARAMETER_1_VALUE>",
            "<YOUR_BATCH_FILTER_PARAMETER_2_KEY>": "<YOUR_BATCH_FILTER_PARAMETER_2_VALUE>",
        }
    },
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name and other arguments directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_year_month"
batch_request.data_connector_query["batch_filter_parameters"] = {
    "year": "2020",
    "month": "01",
}

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1
assert batch_list[0].data.dataframe.shape[0] == 10000

# Here is a BatchRequest naming a configured data_asset representing a filename structure partitioned by year and month.
# This BatchRequest specifies one batch, which is useful for data analysis.
# In addition, the resulting batch is split according to "passenger_count" column with the focus on two-passenger rides.
# Moreover, a randomly sampled fraction of this subset of the batch data is obtained and returned as the final result.
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py batch_request">
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="configured_data_connector_name",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
    data_connector_query={
        "batch_filter_parameters": {
            "<YOUR_BATCH_FILTER_PARAMETER_KEY>": "<YOUR_BATCH_FILTER_PARAMETER_VALUE>",
        }
    },
    batch_spec_passthrough={
        "partitioner_method": "<YOUR_PARTITIONER_METHOD>",
        "partitioner_kwargs": {
            "<YOUR_PARTITIONER_OBJECTIVE_NAME>": "<YOUR_PARTITIONER_OBJECTIVE_KEYS>",
            "batch_identifiers": {
                "<YOUR_PARTITIONER_OBJECTIVE_0_KEY>": "<YOUR_PARTITIONER_OBJECTIVE_0_VALUE>",
                "<YOUR_PARTITIONER_OBJECTIVE_1_KEY>": "<YOUR_PARTITIONER_OBJECTIVE_1_VALUE>",
                "<YOUR_PARTITIONER_OBJECTIVE_2_KEY>": "<YOUR_PARTITIONER_OBJECTIVE_2_VALUE>",
                # ...
            },
        },
        "sampling_method": "<YOUR_SAMPLING_METHOD>",
        "sampling_kwargs": {
            "<YOUR_SAMPLING_ARGUMENT_0_NAME>": "<YOUR_SAMPLING_ARGUMENT_0_VALUE>",
            "<YOUR_SAMPLING_ARGUMENT_1_NAME>": "<YOUR_SAMPLING_ARGUMENT_1_VALUE>",
            "<YOUR_SAMPLING_ARGUMENT_2_NAME>": "<YOUR_SAMPLING_ARGUMENT_2_VALUE>",
            # ...
        },
    },
)
# </snippet>

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name and other arguments directly in the BatchRequest above.
batch_request.data_asset_name = "taxi_data_year_month"
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py batch_filter_parameters">
batch_request.data_connector_query["batch_filter_parameters"] = {
    "year": "2020",
    "month": "01",
}
# </snippet>
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py split_on_column_value passenger_count">
batch_request.batch_spec_passthrough["partitioner_method"] = "partition_on_column_value"
batch_request.batch_spec_passthrough["partitioner_kwargs"] = {
    "column_name": "passenger_count",
    "batch_identifiers": {"passenger_count": 2},
}
# </snippet>
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py sample_using_random 10 pct">
batch_request.batch_spec_passthrough["sampling_method"] = "sample_using_random"
batch_request.batch_spec_passthrough["sampling_kwargs"] = {"p": 1.0e-1}
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py sampling batch size">
batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1
assert batch_list[0].data.dataframe.shape[0] < 200
# </snippet>

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, gx.validator.validator.Validator)
assert "taxi_datasource" in [ds["name"] for ds in context.list_datasources()]
assert "yellow_tripdata_sample_2019-01.csv" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "default_inferred_data_connector_name"
    ]
)