# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - full example">
from pyspark.sql import SparkSession

import great_expectations as gx

context = gx.get_context(mode="file")

# Retrieve the Data Source
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - retrieve Data Source">
data_source_name = "my_data_source"
data_source = context.data_sources.get(name=data_source_name)
# </snippet>

# Add the dataframe as a Data Asset
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - define Data Asset parameters">
# Read in the dataframe
csv = "data/sampled_yellow_tripdata_2019-01.csv"
spark = SparkSession.builder.appName("Read CSV").getOrCreate()
dataframe = spark.read.csv(csv, header=True, inferSchema=True)
# Define a name for the Data Asset
data_asset_name = "spark_dataframe"
# </snippet>

# Create the Data Asset
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - create Data Asset">
data_asset = data_source.add_dataframe_asset(name=data_asset_name, dataframe=dataframe)
# </snippet>

# Add a Batch Definition
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - add Batch Definition">
batch_definition_name = "dataframe_batch"
batch_definition = data_asset.add_batch_definition(name=batch_definition_name)
# </snippet>

# Verify the Batch Definition
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_spark_df_data_asset.py - verify Batch Definition">
batch = batch_definition.get_batch()
print(batch.head())
# </snippet>
# </snippet>
