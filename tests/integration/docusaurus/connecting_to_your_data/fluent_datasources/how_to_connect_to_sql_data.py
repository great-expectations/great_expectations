"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_sql_data" tests/integration/test_script_runner.py --postgresql
```
"""
import tests.test_utils as test_utils
import great_expectations as gx

context = gx.get_context()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py sql_connection_string">
connection_string = "postgresql+psycopg2://username:my_password@localhost/test"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py connection_string">
connection_string = (
    "postgresql+psycopg2://<USERNAME>:${MY_PASSWORD}@<HOST>:<PORT>/<DATABASE>"
)
# </snippet>

connection_string = test_utils.get_default_postgres_url()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py add_sql">
datasource = context.sources.add_sql(
    name="my_datasource", connection_string=connection_string
)
# </snippet>

assert datasource
assert datasource.name == "my_datasource"
