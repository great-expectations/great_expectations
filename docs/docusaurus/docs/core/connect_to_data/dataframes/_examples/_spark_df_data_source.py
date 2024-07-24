# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - full example">
import great_expectations as gx

context = gx.get_context(mode="file")

# Define the Data Source parameters
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - define Data Source parameters">
data_source_name = "my_data_source"
# </snippet>

# Create the Data Source
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_source.py - create Data Source">
data_source = context.data_sources.add_spark(name=data_source_name)
# </snippet>
# </snippet>
