# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_pandas.py - full example">
import great_expectations as gx

context = gx.get_context()

# Define the Data Source's parameters:
data_source_name = "my_filesystem_data_source"
azure_options = {
    "account_url": "${AZURE_STORAGE_ACCOUNT_URL}",
    "credential": "${AZURE_CREDENTIAL}",
}

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_data_source/_abs/_pandas.py - add Data Source">
# Create the Data Source:
data_source = context.data_sources.add_pandas_abs(
    name=data_source_name, azure_options=azure_options
)
# </snippet>
# </snippet>

# Retrieve the Data Source:
data_source = context.data_sources.get(data_source_name)
