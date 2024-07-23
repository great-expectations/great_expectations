# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - full example">
import great_expectations as gx

context = gx.get_context(mode="file")

# Define the Data Source parameters
data_source_name = "my_data_source"

# Create the Data Source
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - create Data Source">
data_source = context.data_sources.add_pandas(name=data_source_name)
# </snippet>
# </snippet>
