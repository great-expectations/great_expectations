import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.datasource.new_datasource import Datasource

config_variables_yaml = """
my_postgres_db_yaml_creds:
  drivername: postgresql
  host: localhost
  port: 5432
  username: postgres
  password: ${MY_DB_PW}
  database: postgres
"""

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
config_variables_file_path: uncommitted/config_variables.yml
"""

datasources_yaml = """
datasources:
  my_postgres_db:
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
      module_name: great_expectations.execution_engine
      class_name: SqlAlchemyExecutionEngine
      credentials: ${my_postgres_db_yaml_creds}
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
  my_other_postgres_db:
    class_name: Datasource
    module_name: great_expectations.datasource
    execution_engine:
      module_name: great_expectations.execution_engine
      class_name: SqlAlchemyExecutionEngine
      credentials:
        drivername: ${POSTGRES_DRIVERNAME}
        host: ${POSTGRES_HOST}
        port: ${POSTGRES_PORT}
        username: ${POSTGRES_USERNAME}
        password: ${POSTGRES_PW}
        database: ${POSTGRES_DB}
    data_connectors:
      default_inferred_data_connector_name:
        class_name: InferredAssetSqlDataConnector
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
    context = ge.get_context()
    context_config_variables_relative_file_path = os.path.join(
        context.GE_UNCOMMITTED_DIR, "config_variables.yml"
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
    my_postgres_db = context.add_datasource(
        name="my_postgres_db", **datasources["datasources"]["my_postgres_db"]
    )
    my_other_postgres_db = context.add_datasource(
        name="my_other_postgres_db",
        **datasources["datasources"]["my_other_postgres_db"]
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
                    "password": "password",
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
                    "password": "",
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
