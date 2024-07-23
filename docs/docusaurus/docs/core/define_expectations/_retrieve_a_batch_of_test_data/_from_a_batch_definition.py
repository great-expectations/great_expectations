from great_expectations import gx

context = gx.get_context()

data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
batch_definition_name = "yellow_tripdata_sample_daily"
batch_definition = (
    context.get_data_source(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_daily.py - retrieve and verify Batch">
batch = batch_definition.get_batch(
    batch_parameters={"year": 2020, "month": 1, "day": 14}
)
batch.head()
