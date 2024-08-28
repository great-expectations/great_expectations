# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - full example">
import great_expectations as gx

context = gx.get_context()

# Define the Data Source's parameters:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - define Data Source parameters">
data_source_name = "my_filesystem_data_source"
bucket_or_name = "test_docs_data"
gcs_options = {}
# </snippet>

# Create the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_spark.py - add Data Source">
data_source = context.data_sources.add_spark_gcs(
    name=data_source_name, bucket_or_name=bucket_or_name, gcs_options=gcs_options
)
# </snippet>
# </snippet>

# Retrieve the Data Source:
data_source_name = "my_filesystem_data_source"
data_source = context.data_sources.get(data_source_name)
