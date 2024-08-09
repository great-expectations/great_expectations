# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py - full example">
import great_expectations as gx

context = gx.get_context()

# Define the Data Source's parameters:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py - define Data Source parameters">
data_source_name = "my_filesystem_data_source"
bucket_name = "superconductive-docs-test"
boto3_options = {}
# </snippet>

# Create the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_s3/_spark.py - add Data Source">
data_source = context.data_sources.add_spark_s3(
    name=data_source_name, bucket=bucket_name, boto3_options=boto3_options
)
# </snippet>
# </snippet>
