# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - full_example">
import great_expectations as gx

context = gx.get_context()

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - retrieve Data Asset">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
file_data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - add Batch Definition">
batch_definition_name = "yellow_tripdata_sample_2019-01.csv"
batch_definition_path = "folder_with_data/yellow_tripdata_sample_2019-01.csv"

batch_definition = file_data_asset.add_batch_definition_path(
    name=batch_definition_name, path=batch_definition_path
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - retrieve Batch and verify">
batch = batch_definition.get_batch()
print(batch.head())
# </snippet>
# </snippet>
