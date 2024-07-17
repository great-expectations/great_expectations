import great_expectations as gx

# This example uses a File Data Context
#  which already has a Data Source configured.
context = gx.get_context()

# Retrieve the Data Source
data_source_name = "my_filesystem_data_source"
data_source = context.get_datasource(data_source_name)

# Define the Data Asset's parameters:
asset_name = "abs_directory_asset"
abs_container = "my_container"
abs_name_starts_with = "data/taxi_yellow_tripdata_samples/"

# Add the Data Asset to the Data Source:
directory_csv_asset = data_source.add_directory_csv_asset(
    name=asset_name,
    abs_container=abs_container,
    abs_name_starts_with=abs_name_starts_with,
)
