import os

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.fluent.config_str import ConfigStr
from tests.test_utils import load_data_into_test_database

yaml = YAMLHandler()

"""
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py export_env_vars">
export MY_DB_PW=password
export POSTGRES_CONNECTION_STRING=postgresql://postgres:${MY_DB_PW}@localhost:5432/postgres
# </snippet>
"""

"""
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py config_variables_yaml">
my_postgres_db_yaml_creds: postgresql://localhost:${MY_DB_PW}@$localhost:5432/postgres
# </snippet>
"""

# Override without snippet tag
config_variables_yaml = """
my_postgres_db_yaml_creds: postgresql://postgres:${MY_DB_PW}@localhost:5432/postgres
"""

config_variables_file_path = """
config_variables_file_path: uncommitted/config_variables.yml
"""

# add test_data to database for testing
load_data_into_test_database(
    table_name="postgres_taxi_data",
    csv_path="./data/yellow_tripdata_sample_2019-01.csv",
    connection_string="postgresql://postgres:${MY_DB_PW}@localhost:5432/postgres",
)
env_vars = []

# set environment variables using os.environ()
os.environ["MY_DB_PW"] = "password"
os.environ[
    "POSTGRES_CONNECTION_STRING"
] = "postgresql://postgres:${MY_DB_PW}@localhost:5432/postgres"

# get context and set config variables in config_variables.yml
context = gx.get_context()
context_config_variables_relative_file_path = os.path.join(
    context.GX_UNCOMMITTED_DIR, "config_variables.yml"
)

context_config_variables_file_path = os.path.join(
    context.root_directory, context_config_variables_relative_file_path
)
# write content to config_variables.yml
with open(context_config_variables_file_path, "w+") as f:
    f.write(config_variables_yaml)

# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py add_credentials_as_connection_string">
# The password can be added as an environment variable
pg_datasource = context.sources.add_or_update_sql(
    name="my_postgres_db",
    connection_string="postgresql://postgres:${MY_DB_PW}@localhost:5432/postgres",
)

# Alternately, the full connection string can be added as an environment Variable
pg_datasource = context.sources.add_or_update_sql(
    name="my_postgres_db", connection_string="${POSTGRES_CONNECTION_STRING}"
)
# </snippet>

pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="postgres_taxi_data"
)

assert context.list_datasources() == [
    {
        "type": "sql",
        "name": "my_postgres_db",
        "assets": [
            {
                "name": "postgres_taxi_data",
                "type": "table",
                "order_by": [],
                "batch_metadata": {},
                "table_name": "postgres_taxi_data",
                "schema_name": None,
            }
        ],
        "connection_string": ConfigStr("${POSTGRES_CONNECTION_STRING}"),
    }
]


# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py add_credential_from_yml">
# Variables in config_variables.yml can be referenced in the connection string
pg_datasource = context.sources.add_or_update_sql(
    name="my_postgres_db", connection_string="${my_postgres_db_yaml_creds}"
)
# </snippet>
pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="postgres_taxi_data"
)
assert context.list_datasources() == [
    {
        "type": "sql",
        "name": "my_postgres_db",
        "assets": [
            {
                "name": "postgres_taxi_data",
                "type": "table",
                "order_by": [],
                "batch_metadata": {},
                "table_name": "postgres_taxi_data",
                "schema_name": None,
            }
        ],
        "connection_string": ConfigStr("${my_postgres_db_yaml_creds}"),
    }
]

# unset environment variables
for var in env_vars:
    os.environ.pop(var, None)
