---
title: Credential storage and usage options
---

## Adding Credentials

### 1. Adding a Connection String

Credentials can be added to a Datasource using a connection string, which is equivalent to the URL that is used by SqlAlchemy to create an [`Engine`](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls).
If you are developing interactively in a Notebook, this is the quickest way to get your configuration up and running,
but you run the risk of exposing your credentials.

A Datasource with the following configuration:
 1. connection to PostgreSQL database
 2. username : `postgres`
 3. password : ``
 4. server: `localhost`
 5. database: `test_ci`

would look like this:

```yaml
    name: my_postgres_datasource
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: postgresql+psycopg2://postgres:@localhost/test_ci
    ...

```

### 2. Populating Credentials through an Environment Variable

Decide where you would like to save the desired credentials or config values - in a YAML file, environment variables, or a combination - then save the values. In most cases, we suggest using a config variables YAML file. YAML files make variables more visible, easily editable, and allow for modularization (e.g. one file for dev, another for prod).

If using a YAML file, save desired credentials or config values to `great_expectations/uncommitted/config_variables.yml` or another YAML file of your choosing:

```yaml
my_postgres_db_yaml_creds:
  drivername: postgres
  host: 127.0.0.778
  port: '7987'
  username: administrator
  password: ${MY_DB_PW}
  database: postgres
  ...
```

:::warning
  Add content from Existing Document
  [Link to Existing Document](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html#how-to-guides-configuring-data-contexts-how-to-use-a-yaml-file-or-environment-variables-to-populate-credentials)
:::


### 3. Populating Credentials from a Secrets store

:::warning
  Add content from Existing Document
  [Link to Existing Document](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_contexts/how_to_populate_credentials_from_a_secrets_store.html?highlight=credentials)
:::
