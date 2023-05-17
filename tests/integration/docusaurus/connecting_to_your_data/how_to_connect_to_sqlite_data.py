# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data import">
import great_expectations as gx

context = gx.get_context()
# </snippet>

assert context is not None 

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data connection_string">
my_connection_string = "sqlite:///<PATH_TO_DB_FILE>"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data datasource_name">
datasource_name = "my_datasource"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data datasource">
datasource = context.sources.add_sqlite(name=datasource_name, connection_string=my_connection_string)
# </snippet>

assert "base_directory:" in str(datasource)
assert "name: my_datasource" in str(datasource)
assert "type: sqlite" in str(datasource)


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data assest_name">
asset_name = "my_asset"
asset_table_name = "yellow_tripdata_sample"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data table_asset">
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
# </snippet>

assert table_asset is not None


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data asset_query">
asset_name = "my_asset"
asset_query = "SELECT * from yellow_tripdata_sample"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data query_table_asset">
table_asset = datasource.add_query_asset(name=asset_name, query=asset_query)
# </snippet>

assert table_asset is not None
