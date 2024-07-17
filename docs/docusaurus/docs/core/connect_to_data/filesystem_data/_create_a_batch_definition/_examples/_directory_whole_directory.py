# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - full_example">
from great_expectations import gx

context = gx.get_context()

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve Data Asset">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
file_data_asset = context.get_data_source(data_source_name).get_asset(data_asset_name)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - add Batch Definition">
batch_definition_name = "yellow_tripdata_sample_2019-01.csv"
batch_definition = file_data_asset.add_batch_definition_whole_directory(
    name=batch_definition_name
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve Batch and verify">
batch = batch_definition.get_batch()
batch.head()
# </snippet>
# </snippet>
