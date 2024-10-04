# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_directory_asset.py - full example">
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_directory_asset.py - retrieve Data Source">
import great_expectations as gx

# This example uses a File Data Context which already has
#  a Data Source defined.
context = gx.get_context()
data_source_name = "my_filesystem_data_source"
data_source = context.data_sources.get(data_source_name)
# </snippet>

# Define the Data Asset's parameters:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_directory_asset.py - define Data Asset parameters">
asset_name = "gcs_taxi_csv_directory_asset"
gcs_prefix = "data/taxi_yellow_tripdata_samples/"
data_directory = "data/taxi_yellow_tripdata_samples/"
# </snippet>

# Add the Data Asset to the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_asset/_gcs/_directory_asset.py - add Data Asset">
directory_csv_asset = data_source.add_directory_csv_asset(
    name=asset_name, gcs_prefix=gcs_prefix, data_directory=data_directory
)
# </snippet>
# </snippet>

# Use the Data Context to retrieve the Data Asset when needed:
data_source_name = "my_filesystem_data_source"
asset_name = "gcs_taxi_csv_directory_asset"
directory_csv_asset = context.data_sources.get(data_source_name).get_asset(asset_name)
