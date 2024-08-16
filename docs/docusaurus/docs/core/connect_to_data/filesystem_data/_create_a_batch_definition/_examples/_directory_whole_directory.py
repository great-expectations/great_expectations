# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - full_example">
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve Data Asset">
from pathlib import Path

import great_expectations as gx

context = gx.get_context(mode="ephemeral")

data_source_name = "my_filesystem_data_source"
data_asset_name = "my_directory_data_asset"
# file_data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
data_source = context.data_sources.add_spark_filesystem(data_source_name, base_directory=Path("data"))
file_data_asset = data_source.add_directory_csv_asset(data_asset_name, data_directory=Path("folder_with_data"), header=True)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - add Batch Definition">
batch_definition_name = "yellow_tripdata"
batch_definition = file_data_asset.add_batch_definition_whole_directory(
    name=batch_definition_name
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve and verify Batch">
batch = batch_definition.get_batch()
batch.head()
# </snippet>
# </snippet>
