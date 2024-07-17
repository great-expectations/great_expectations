# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - full example">
from great_expectations import gx

context = gx.get_context()

data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
file_data_asset = context.get_data_source(data_source_name).get_asset(data_asset_name)

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - define Batch Definition parameters">
batch_definition_name = "yellow_tripdata_sample_monthly"
batch_definition_column = "pickup_datetime"
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - add Batch Definition">
batch_definition = file_data_asset.add_batch_definition_monthly(
    name=batch_definition_name, column=batch_definition_column
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - retrieve and verify Batch">
batch = batch_definition.get_batch(batch_parameters={"year": 2020, "month": 1})
batch.head()
# </snippet>
# </snippet>
