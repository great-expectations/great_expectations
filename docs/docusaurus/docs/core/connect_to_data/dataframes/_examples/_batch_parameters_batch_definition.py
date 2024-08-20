# Define Batch Parameters for a Spark dataframe
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - spark dataframe">
from pyspark.sql import SparkSession

csv = "./data/folder_with_data/yellow_tripdata_sample_2019-01.csv"
spark = SparkSession.builder.appName("Read CSV").getOrCreate()
dataframe = spark.read.csv(csv, header=True, inferSchema=True)

batch_parameters = {"dataframe": dataframe}
# </snippet>

# Define Batch Parameters for a pandas dataframe
# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - pandas dataframe">
import pandas

csv_path = "./data/folder_with_data/yellow_tripdata_sample_2019-01.csv"
dataframe = pandas.read_csv(csv_path)

# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - batch parameters example">
batch_parameters = {"dataframe": dataframe}
# </snippet>
# </snippet>


def setup_context_for_example(context):
    data_source = context.data_sources.add_pandas(name="my_data_source")
    data_asset = data_source.add_dataframe_asset(name="my_dataframe_data_asset")
    data_asset.add_batch_definition_whole_dataframe("my_batch_definition")


# <snippet name="docs/docusaurus/docs/core/connect_to_data/dataframes/_examples/_batch_parameters_batch_definition.py - batch.validate() example">
import great_expectations as gx

context = gx.get_context()
# Hide this
setup_context_for_example(context)

# Retrieve the dataframe Batch Definition
data_source_name = "my_data_source"
data_asset_name = "my_dataframe_data_asset"
batch_definition_name = "my_batch_definition"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)

# Create an Expectation to test
expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", max_value=6, min_value=1
)

# Get the dataframe as a Batch
# highlight-next-line
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

# Test the Expectation
validation_results = batch.validate(expectation)
print(validation_results)
# </snippet>
