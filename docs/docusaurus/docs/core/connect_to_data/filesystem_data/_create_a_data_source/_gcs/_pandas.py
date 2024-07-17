# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_pandas.py - full example">
import great_epectations as gx

context = gx.get_context()

# Define the Data Source's parameters:
data_source_name = "my_gcs_datasource"
bucket_or_name = "my_bucket"
gcs_options = {}

# Create the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_gcs/_pandas.py - add Data Source">
data_source = gx.data_sources.add_pandas_gcs(
    name=data_source_name, bucket_or_name=bucket_or_name, gcs_options=gcs_options
)
# </snippet>
# </snippet>

# Retrieve the Data Source:
data_source_name = "my_datasource"
datasource = context.data_sources.get(data_source_name)
