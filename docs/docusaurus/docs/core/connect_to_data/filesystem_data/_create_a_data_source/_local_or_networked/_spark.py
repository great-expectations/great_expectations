# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - full example">
import great_expectations as gx

context = gx.get_context()

# Define the Data Source's parameters:
# This path is relative to the `base_directory` of the Data Context.
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - define Data Source parameters">
source_folder = "./data"
data_source_name = "my_filesystem_data_source"
# </snippet>

# Create the Data Source:
# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_local_or_networked/_spark.py - add Data Source">
data_source = context.data_sources.add_spark_filesystem(
    name=data_source_name, base_directory=source_folder
)
# </snippet>
# </snippet>
