import os

import great_expectations as gx
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.new_datasource import Datasource

yaml = YAMLHandler()

"""
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py config_variables_yaml">
my_postgres_db_yaml_creds: postgresql://localhost:${MY_DB_PW}@$localhost:5432/postgres
# </snippet>
"""

# Override without snippet tag
config_variables_yaml = """
my_postgres_db_yaml_creds: postgresql://localhost:${MY_DB_PW}@$localhost:5432/postgres
"""

"""
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py export_env_vars">
export POSTGRES_DRIVERNAME=postgresql
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USERNAME=postgres
export POSTGRES_PW=
export POSTGRES_DB=postgres
export MY_DB_PW=password
# </snippet>
"""

# Override without snippet tag
export_env_vars = """
export POSTGRES_DRIVERNAME=postgresql
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USERNAME=postgres
export POSTGRES_PW=
export POSTGRES_DB=postgres
export MY_DB_PW=password
"""

config_variables_file_path = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py config_variables_file_path">
config_variables_file_path: uncommitted/config_variables.yml
# </snippet>
"""

datasources_yaml = """
# <snippet name="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py datasources_yaml">
fluent_datasources:
    my_postgres_db:
        type: postgres
        connection_string: ${my_postgres_db_yaml_creds}
        assets:
            my_first_table_asset:
                type: table
                table_name: my_first_table
    my_other_postgres_db:
        type: postgres
        connection_string: postgres+${POSTGRES_DRIVERNAME}://${POSTGRES_USERNAME}:${POSTGRES_PW}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
        assets:
            my_second_table_asset:
                type: table
                table_name: my_second_table
# </snippet>
"""

# NOTE: The following code is only for testing and can be ignored by users.
env_vars = []
try:
    # set environment variables using export_env_vars
    for line in export_env_vars.split("export"):
        if line.strip() != "":
            key, value = line.split("=")[0].strip(), line.split("=")[1].strip()
            env_vars.append(key)
            os.environ[key] = value

    # get context and set config variables in config_variables.yml
    context = gx.get_context()
    context_config_variables_relative_file_path = os.path.join(
        context.GX_UNCOMMITTED_DIR, "config_variables.yml"
    )
    assert (
        yaml.load(config_variables_file_path)["config_variables_file_path"]
        == context_config_variables_relative_file_path
    )
    context_config_variables_file_path = os.path.join(
        context.root_directory, context_config_variables_relative_file_path
    )
    with open(context_config_variables_file_path, "w+") as f:
        f.write(config_variables_yaml)

    # add datsources now that variables are configured
    datasources = yaml.load(datasources_yaml)
    context.sources.add_sql(
        name="my_postgres_db", connection_string=os.get("my_postgres_db_yaml_creds")
    )
    assert type(my_postgres_db) == Datasource
    assert type(my_other_postgres_db) == Datasource
    assert context.list_datasources() == [
        {
            "execution_engine": {
                "credentials": {
                    "drivername": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "username": "postgres",
                    "password": "***",
                    "database": "postgres",
                },
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
            },
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                }
            },
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "name": "my_postgres_db",
        },
        {
            "execution_engine": {
                "credentials": {
                    "drivername": "postgresql",
                    "host": "localhost",
                    "port": "5432",
                    "username": "postgres",
                    "password": "***",
                    "database": "postgres",
                },
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
            },
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                }
            },
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "name": "my_other_postgres_db",
        },
    ]

except Exception:
    raise
finally:
    # unset environment variables if they were set
    for var in env_vars:
        os.environ.pop(var, None)
