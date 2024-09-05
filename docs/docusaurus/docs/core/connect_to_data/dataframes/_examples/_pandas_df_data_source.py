# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - full example">
import great_expectations as gx

# Retrieve your Data Context
context = gx.get_context()
# Hide this
assert type(context).__name__ == "EphemeralDataContext"

# Define the Data Source name
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - define Data Source name">
data_source_name = "my_data_source"
# </snippet>

# Add the Data Source to the Data Context
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py Add Data Source">
data_source = context.data_sources.add_pandas(name=data_source_name)
# Hide this
assert data_source.name == data_source_name
# </snippet>
# </snippet>
