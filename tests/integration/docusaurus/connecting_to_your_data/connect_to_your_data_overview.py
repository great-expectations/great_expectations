# <snippet name="tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview add_datasource">
import great_expectations as gx

context = gx.get_context()
context.sources.add_pandas_filesystem(
    name="my_pandas_datasource", base_directory="./data"
)
# </snippet>

assert "my_pandas_datasource" in context.datasources


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/connect_to_your_data_overview config">
datasource = context.datasources["my_pandas_datasource"]
print(datasource)
# </snippet>

assert "base_directory:" in str(datasource)
assert "name: my_pandas_datasource" in str(datasource)
assert "type: pandas_filesystem" in str(datasource)
