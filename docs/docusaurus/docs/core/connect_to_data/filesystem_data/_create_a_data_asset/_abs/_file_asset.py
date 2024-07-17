import great_expectations as gx

# This example uses a File Data Context
#  which already has a Data Source configured.
context = gx.get_context()

# Retrieve the Data Source
data_source_name = "my_filesystem_data_source"
data_source = context.get_datasource(data_source_name)

# Define the Data Asset's parameters:
asset_name = "abs_file_csv_asset"

# Add the Data Asset to the Data Source:
file_asset = data_source.add_csv_asset(name=asset_name)
