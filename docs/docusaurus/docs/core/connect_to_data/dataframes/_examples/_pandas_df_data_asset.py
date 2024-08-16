# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
assert type(context).__name__ == "EphemeralDataContext"
# Hide this
# SETUP FOR THE EXAMPLE:
# Hide this
data_source = context.data_sources.add_pandas(name="my_data_source")

# Retrieve the Data Source
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - retrieve Data Source">
data_source_name = "my_data_source"
data_source = context.data_sources.get(data_source_name)
# </snippet>

# Define the Data Asset name
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - define Data Asset name">
data_asset_name = "my_dataframe_data_asset"
# </snippet>

# Add a Data Asset to the Data Source
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_asset.py - add Data Asset">
data_asset = data_source.add_dataframe_asset(name=data_asset_name)
# </snippet>

# Hide this
assert data_asset.name == data_asset_name
# </snippet>
