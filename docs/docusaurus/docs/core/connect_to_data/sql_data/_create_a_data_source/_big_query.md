import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import PreReqPython from '../../../_core_components/prerequisites/_python_installation.md'
import PreReqGxInstalledWithSqlDependecies from '../../../_core_components/prerequisites/_gx_installation_with_sql_dependencies.md'
import PreReqDataContext from '../../../_core_components/prerequisites/_preconfigured_data_context.md'
import PreReqCredentials from '../../../_core_components/prerequisites/_securely_configured_credentials.md'

### Prerequisites
- <PreReqPython/>.
- <PreReqGxInstalledWithSqlDependecies/>
- <PreReqDataContext/>.
- <PreReqCredentials/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import GX and instantiate a Data Context:

   ```python
   import great_expectations as gx
   context = gx.get_context()
   ```

2. Define a name and connection string for your Data Source.

   All Data Sources in your Data Context should have unique names.  Other than that, you can assign any name to a Data Source.  You should use a descriptive name to help you remember the Data Sources purpose.

   Your connection string or credentials should not be saved in plain text in a variable.  Instead, you should reference a securely stored connection string or credentials through string substitution.  The guidance on how to [Configure your credentials](#configure-your-credentials) covers how to determine the format of your connection string, securely store your connection string or credentials, and how to reference your connection string or credentials in your scripts.

   The following code defines a Data Source name and references a connection string that has been securely stored in its entirety:

    ```python title="Python"
   datasource_name = "nyc_taxi_data"
   my_connection_string = "${BIGQUERYSQL_CONNECTION_STRING}"
   ```

3. Create a BigQuery SQL Data Source:

   ```python title="Python"
   datasource = context.sources.add_postgres(name=datasource_name, connection_string=my_connection_string)
   ```

4. Optional. Verify the Data Source is connected:

   ```python
   print(context.datasources)
   ```
   
   A list of Data Sources is printed.  You can verify your Data Source was created and added to the Data Context by checking for its name in the printed list.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Sample code"
import great_expectations as gx
context = gx.get_context()

# You only need to define one of these:
my_connection_string = "postgresql+psycopg2://${USERNAME}:${PASSWORD}@<host>:<port>/<database>"
my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

datasource_name = "my_sql_data_source"
datasource = context.sources.add_postgres(
      name=datasource_name,
      connection_string=my_connection_string
   )
```

</TabItem>

</Tabs>