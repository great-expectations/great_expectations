from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.expectations.metrics.import_manager import sa

context = ge.get_context()

datasource_yaml = f"""
name: taxi_datasource
class_name: SimpleSqlalchemyDatasource
connection_string: <CONNECTION_STRING>

introspection:  # Each key in the "introspection" section is an InferredAssetSqlDataConnector
    whole_table:
        introspection_directives:
            include_views: true
        skip_inapplicable_tables: true  # skip and continue upon encountering introspection errors
        excluded_tables:  # a list of tables to ignore when inferring data asset_names
            - main.yellow_tripdata_sample_2019_03  # format: schema_name.table_name

    daily:
        introspection_directives:
            include_views: true
        skip_inapplicable_tables: true  # skip and continue upon encountering introspection errors
        included_tables:  # only include tables in this list when inferring data asset_names
            - main.yellow_tripdata_sample_2019_01  # format: schema_name.table_name
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: pickup_datetime
            date_format_string: "%Y-%m-%d"

    hourly:
        introspection_directives:
            include_views: true
        skip_inapplicable_tables: true
        included_tables:  # only include tables in this list when inferring data asset_names
            - main.yellow_tripdata_sample_2019_01  # format: schema_name.table_name
        splitter_method: _split_on_converted_datetime
        splitter_kwargs:
            column_name: pickup_datetime
            date_format_string: "%Y-%m-%d %H"

tables:  # Each key in the "tables" section is a table_name (key name "tables" in "SimpleSqlalchemyDatasource" configuration is reserved).
    # data_asset_name is: concatenate(data_asset_name_prefix, table_name, data_asset_name_suffix)
    yellow_tripdata_sample_2019_01:  # Must match table name exactly.
        partitioners:  # Each key in the "partitioners" sub-section the name of a ConfiguredAssetSqlDataConnector (key name "partitioners" in "SimpleSqlalchemyDatasource" configuration is reserved).
            whole_table:
                include_schema_name: True
                data_asset_name_prefix: taxi__
                data_asset_name_suffix: __asset

            by_num_riders:
                include_schema_name: True
                data_asset_name_prefix: taxi__
                data_asset_name_suffix: __asset
                splitter_method: _split_on_column_value
                splitter_kwargs:
                    column_name: passenger_count

            by_num_riders_random_sample:
                include_schema_name: True
                data_asset_name_prefix: taxi__
                data_asset_name_suffix: __asset
                splitter_method: _split_on_column_value
                splitter_kwargs:
                    column_name: passenger_count
                sampling_method: _sample_using_random
                sampling_kwargs:
                    p: 1.0e-1
"""

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the yaml above.
data_dir_path = "data"
CONNECTION_STRING = f"sqlite:///{data_dir_path}/yellow_tripdata.db"

datasource_yaml = datasource_yaml.replace("<CONNECTION_STRING>", CONNECTION_STRING)

context.test_yaml_config(datasource_yaml)

context.add_datasource(**yaml.load(datasource_yaml))
available_data_asset_names = context.datasources[
    "taxi_datasource"
].get_available_data_asset_names(data_connector_names="whole_table")["whole_table"]
assert len(available_data_asset_names) == 2

# Here is a BatchRequest referring to an un-partitioned inferred data_asset.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="whole_table",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019_01"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head(n_rows=10))

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 1
batch_data = batch_list[0].data
num_rows = batch_data.execution_engine.engine.execute(
    sa.select([sa.func.count()]).select_from(batch_data.selectable)
).one()[0]
assert num_rows == 10000

# Here is a BatchRequest naming an inferred data_asset partitioned by day.
# This BatchRequest specifies multiple batches, which is useful for dataset exploration.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="daily",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019_01"

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 31  # number of days in January

# Here is a BatchRequest naming an inferred data_asset partitioned by hour.
# This BatchRequest specifies multiple batches, which is useful for dataset exploration.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="hourly",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name directly in the BatchRequest above.
batch_request.data_asset_name = "yellow_tripdata_sample_2019_01"

batch_list = context.get_batch_list(batch_request=batch_request)
assert 24 <= len(batch_list) <= 744  # number of hours in a 31-day month

# Here is a BatchRequest naming a configured data_asset partitioned by passenger_count.
# This BatchRequest specifies multiple batches, which is useful for dataset exploration and data analysis.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="by_num_riders",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name and other arguments directly in the BatchRequest above.
batch_request.data_asset_name = "taxi__yellow_tripdata_sample_2019_01__asset"

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 6

# Here is a BatchRequest naming a configured data_asset partitioned by passenger_count.
# In addition, a randomly sampled fraction of each partition's batch data is obtained.
# Finally, a randomly sampled fraction of each partition's batch data is selected and returned as the final batch list.
# This BatchRequest specifies multiple batches, which is useful for dataset exploration and data analysis.
batch_request = BatchRequest(
    datasource_name="taxi_datasource",
    data_connector_name="by_num_riders_random_sample",
    data_asset_name="<YOUR_DATA_ASSET_NAME>",
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your data asset name and other arguments directly in the BatchRequest above.
batch_request.data_asset_name = "taxi__yellow_tripdata_sample_2019_01__asset"

batch_list = context.get_batch_list(batch_request=batch_request)
assert len(batch_list) == 6  # ride occupancy ranges from 1 passenger to 6 passengers

batch_data = batch_list[1].data  # 2-passenger sample of batch data
num_rows = batch_data.execution_engine.engine.execute(
    sa.select([sa.func.count()]).select_from(batch_data.selectable)
).scalar()
assert num_rows < 200

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
assert "taxi_datasource" in [ds["name"] for ds in context.list_datasources()]
assert "yellow_tripdata_sample_2019_01" in set(
    context.get_available_data_asset_names()["taxi_datasource"]["whole_table"]
)
assert "taxi__yellow_tripdata_sample_2019_01__asset" in set(
    context.get_available_data_asset_names()["taxi_datasource"][
        "by_num_riders_random_sample"
    ]
)
