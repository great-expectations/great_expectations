"""
To run this test locally, run:
1. Activate docker.
2. From the repo root dir, activate the postgresql database docker container:
cd assets
cd docker
cd postgresql
docker compose up

3. Run the following command from the repo root dir in a second terminal:
pytest  --postgresql --docs-tests -k "create_a_datasource_postgres" tests/integration/test_script_runner.py
"""

# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py full sample code">
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py imports">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py name and connection string">
datasource_name = "my_new_datasource"
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py - example postgresql connection string using string substitution">
my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"
# </snippet>
# </snippet>

# highlight-start
# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py create data source">
data_source = context.data_sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)
# </snippet>
# highlight-end

# <snippet name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py verify data source">
print(context.data_sources.get(datasource_name))
# </snippet>
# </snippet>
