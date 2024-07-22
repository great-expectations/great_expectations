# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - full example">
import pandas as pd

import great_expectations as gx

# Read in dataframe
csv_path = "data/sampled_yellow_tripdata_2019-01.csv"
dataframe = pd.read_csv(csv_path)

# Create Data Source
context = gx.get_context(mode="file")

data_source_name = "my_data_source"
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - create Data Source">
data_source = context.data_sources.add_pandas(name=data_source_name)
# </snippet>


# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - define Data Asset parameters">
data_asset_name = "pandas_dataframe"
# </snippet>
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_pandas_df_data_source.py - add Data Asset">
data_asset = data_source.add_dataframe_asset(name=data_asset_name, dataframe=dataframe)
# </snippet>

# We intend to have helper methods for creating different types of batch definitions
# but the raw "add_batch_definition" will use the whole df as the default.
batch_definition = data_asset.add_batch_definition(name="bd")

# Verify
batch = batch_definition.get_batch()
print(batch.head())
# </snippet>
