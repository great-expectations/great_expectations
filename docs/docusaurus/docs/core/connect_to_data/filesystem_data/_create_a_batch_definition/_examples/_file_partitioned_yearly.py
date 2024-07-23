# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - full_example">
from great_expectations import gx

context = gx.get_context()

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - retrieve Data Asset">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
file_data_asset = context.get_data_source(data_source_name).get_asset(data_asset_name)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - add Batch Definition">
batch_definition_name = "yearly_yellow_tripdata_sample"
batch_definition_regex = r"yellow_tripdata_sample_(?P<year>\d{4})\.csv"

batch_definition = file_data_asset.add_batch_definition_yearly(
    name=batch_definition_name, regex=batch_definition_regex
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - retrieve Batch and verify">
batch = batch_definition.get_batch(batch_parameters={"year": 2020})
batch.head()
# </snippet>
# </snippet>
