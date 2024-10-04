"""
To run this test locally, run:
1. Activate docker.
2. From the repo root dir, activate the postgresql database docker container:

pytest  --postgresql --docs-tests -k "create_a_data_asset_postgres" tests/integration/test_script_runner.py
"""

# The following import and setup method sets up the data and environment used to test this example.
# It can be disregarded by anyone using referencing this script as an example of a
# GX workflow.
from docs.docusaurus.docs.components._testing.utility_scripts.postgres_data_setup import (
    setup,
)

setup()

# The example starts here
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py full code">
import great_expectations as gx

context = gx.get_context()

# Set up: Create a Data Source
datasource_name = "my_new_datasource"
my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

data_source = context.data_sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)

print(context.data_sources.get(datasource_name))

# Alternatively, fetch a Data Source from the Data Context.
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py retrieve data source">
data_source_name = "my_new_datasource"
data_source = context.data_sources.get(data_source_name)
# </snippet>

# Example of creating a Table Asset
# highlight-start
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py table asset">
asset_name = "MY_TABLE_ASSET"
database_table_name = "postgres_taxi_data"
table_data_asset = data_source.add_table_asset(
    table_name=database_table_name, name=asset_name
)
# </snippet>
# highlight-end

# Example of creating a Query Asset
# highlight-start
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py query asset">
asset_name = "MY_QUERY_ASSET"
asset_query = "SELECT * from postgres_taxi_data"
query_data_asset = data_source.add_query_asset(query=asset_query, name=asset_name)
# </snippet>
# highlight-end

# Verify that the Data Assets were created
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py verify asset">
print(data_source.assets)
# </snippet>
# </snippet>
