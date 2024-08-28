# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
assert type(context).__name__ == "EphemeralDataContext"
# Hide this
# SETUP FOR THE EXAMPLE:
# Hide this
data_source = context.data_sources.add_pandas(name="my_data_source")
# Hide this
data_asset = data_source.add_dataframe_asset(name="my_dataframe_data_asset")

# Retrieve the Data Asset
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - retrieve Data Asset">
data_source_name = "my_data_source"
data_asset_name = "my_dataframe_data_asset"
data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
# </snippet>

# Define the Batch Definition name
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - define Batch Definition name">
batch_definition_name = "my_batch_definition"
# </snippet>

# Add a Batch Definition to the Data Asset
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_batch_definition.py - add Batch Definition">
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)
# </snippet>
# Hide this
assert batch_definition.name == batch_definition_name
# </snippet>
