# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_file_asset.py - full example">
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_file_asset.py - retrieve Data Source">
import great_expectations as gx

# This example uses a File Data Context
#  which already has a Data Source configured.
context = gx.get_context()
data_source_name = "my_filesystem_data_source"
data_source = context.data_sources.get(data_source_name)
# </snippet>

# Define the Data Asset's parameters:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_file_asset.py - define Data Asset parameters">
asset_name = "abs_file_csv_asset"
abs_container = "superconductive-public"
abs_prefix = "data/taxi_yellow_tripdata_samples/"
# </snippet>

# Add the Data Asset to the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_abs/_file_asset.py - add Data Asset">
file_asset = data_source.add_csv_asset(
    name=asset_name, abs_container=abs_container, abs_name_starts_with=abs_prefix
)
# </snippet>
# </snippet>
