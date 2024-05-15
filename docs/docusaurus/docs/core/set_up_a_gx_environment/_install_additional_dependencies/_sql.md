import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';

To validate data stored on SQL databases with {GxData.product_name}, you create your GX Python environment, install {GxData.product_name} locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation

1. Run the following pip command to install optional dependencies for `SQLAlchemy`:

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[sqlalchemy]'
   ```

   To install dependencies for a different SQL dialect, use the corresponding command from [SQL dialect dependency commands](#sql-dialect-dependency-commands).

2. Configure an environment variable with the credentials to access your SQL database.

   You can manage your credentials by storing them as environment variables.  To do this, enter `export ENV_VARIABLE_NAME=env_var_value` in the terminal or add the equivalent command to your `~/.bashrc` file. For example:

   ```bash title='Terminal input'
   export MY_DB_PW=<MY_PASSWORD>
   ```

   When entering this command, replace `<MY_PASSWORD>` with your SQL database password.

   Once your password is stored in an environment variable, you can reference it in GX using string substitution.  For instance, if you were accessing a PostgreSql database, you could provide a connection string that uses `${MY_DB_PW}` to insert your password from the environment variable:

   ```bash title="Example PostgreSql connection string"
    "postgresql+psycopg2://<username>:${MY_DB_PW}@<host>:<port>/<database>"
   ```

   :::info 
  
   If you do not want to store your credentials as environment variables, you can [store them in the file `config_variables.yml`](/core/installation_and_setup/manage_credentials.md#yaml-file) after you have [created a File Data Context](/core/installation_and_setup/manage_data_contexts.md?context-type=file#initialize-a-new-data-context).
  
   :::


## SQL dialect dependency commands

The following table lists the installation commands used to install {GxData.product_name} dependencies for specific SQL dialects. These dependencies are required for the successful operation of {GxData.product_name}.

| SQL Dialect | Command |
| :-- | :-- | 
| AWS Athena | `pip install 'great_expectations[athena]'` |
| BigQuery | `pip install 'great_expectations[bigquery]'` |
| MSSQL | `pip install 'great_expectations[mssql]'` |
| PostgreSQL | `pip install 'great_expectations[postgresql]'` |
| Redshift | `pip install 'great_expectations[redshift]'` |
| Snowflake | `pip install 'great_expectations[snowflake]'` |
| Trino | `pip install 'great_expectations[trino]'` |