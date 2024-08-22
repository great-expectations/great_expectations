import GxData from '../../_core_components/_data.jsx';
import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import RecommendedVirtualEnvironment from '../../_core_components/prerequisites/_recommended_virtual_environment.md';
import InfoUsingAVirtualEnvironment from '../../_core_components/admonitions/_if_you_are_using_a_virtual_environment.md';
import SqlDialectInstallationCommands from './_sql_dialect_installation_commands.md';

To validate data stored on SQL databases with {GxData.product_name}, you create your GX Python environment, install {GxData.product_name} locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- <RecommendedVirtualEnvironment/>

## Installation

1. Run the pip command to install the dependencies for your data's SQL dialect.

   <SqlDialectInstallationCommands/>
   
   To install dependencies for a specific SQL dialect, use the corresponding command from the table above.

   If you are not using one of the listed dialects, you can install the dependencies for SQLAlchemy with the command:

   ```bash title="Terminal input"
   python -m pip install 'great_expectations[sqlalchemy]'
   ```

2. Configure your SQL database credentials.

   You can store your SQL database password by replacing `<MY_PASSWORD>` with your password in the following command:

   ```bash title='Terminal input'
   export MY_DB_PW=<MY_PASSWORD>
   ```
   
   Or you can store your entire SQL database connection string by replacing `<MY_CONNECTION_STRING>` with it and running:
 
   ```bash title='Terminal input'
   export MY_DB_CONNECTION_STRING=<MY_CONNECTION_STRING>
   ```

   :::info

   You can manage your credentials for all environments and Data Sources by storing them as environment variables.  To do this, enter `export ENV_VARIABLE_NAME=env_var_value` in the terminal or add the equivalent command to your `~/.bashrc` file.

   You can reference environment variables in {GxData.product_name} by including them in strings using the format `${ENV_VARIABLE_NAME}`.  For instance, to insert the password stored as `MY_DB_PASSWORD` into a PostgreSql connection string you would provide the string:

   ```python title="Example PostgreSql Connection String"
   "postgresql+psycopg2://<username>:${MY_DB_PW}@<host>:<port>/<database>"
   ```

   As an alternative to environment variables, you can also [store credentials in the file `config_variables.yml`](/core/configure_project_settings/configure_credentials/configure_credentials.md?storage_type=config_yml) after you have [created a File Data Context](/core/set_up_a_gx_environment/create_a_data_context.md?context_type=file).

   :::
