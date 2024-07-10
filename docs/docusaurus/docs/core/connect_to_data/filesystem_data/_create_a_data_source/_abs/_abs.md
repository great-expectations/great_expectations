import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstall from '../../../../_core_components/prerequisites/_python_installation.md'
import PrereqGxInstall from '../../../../_core_components/prerequisites/_gx_installation_with_s3_dependencies.md'
import PrereqDataContext from '../../../../_core_components/prerequisites/_preconfigured_data_context.md'

### Prerequisites
- <PrereqPythonInstall/>
- <PrereqGxInstall/>
- <PrereqDataContext/>
- Access to data files in Azure Blob Storage.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Define the Data Source's parameters.

   The following information is required when you create a Microsoft Azure Blob Storage Data Source:

   - `name`: A descriptive name used to reference the Data Source.  This should be unique within the Data Context.
   - `azure_options`: Authentication settings.
   
   The `azure_options` parameter accepts a dictionary which should include two keys: `credential` and either `account_url` or `conn_str`.

   - `credential`: An Azure Blob Storage token
   - `account_url`: The url of your Azure Blob Storage account.  If you provide this then `conn_str` should be left out of the dictionary.
   - `conn_str`: The connection string for your Azure Blob Storage account.  If you provide this then `account_url` should not be included in the dictionary.

   To keep your credentials secure you can define them as environment variables or entries in `config_variables.yml`.  For more information on secure storage and retrieval of credentials in GX see [Configure credentials](/core/connect_to_data/sql_data/sql_data.md#configure-credentials).

   Update the variables in the following code and execute it to define `name` and `azure_options`.  In this example, the value for `account_url` is pulled from the environment variable `AZURE_STORAGE_ACCOUNT_URL` and the value for `credential` is pulled from the environment variable `AZURE_CREDENTIAL`:

   ```python title="Python"
   datasource_name = "nyc_taxi_data"
   azure_options = {
      "account_url": "${AZURE_STORAGE_ACCOUNT_URL}",
      "credential": "${AZURE_CREDENTIAL}",
   }
   ```

2. Add an Azure Blob Storage Data Source to your Data Context.

   GX can leverage either pandas or Spark as the backend for your Azure Blob Storage Data Source.  To create your Data Source, execute one of the following sets of code:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas">

   ```python title="Python"
   data_source = gx.data_sources.add_pandas_abs(
      name=data_source_name,
      azure_options=azure_options
   )
   ```

   </TabItem>

   <TabItem value="spark" label="Spark">

   ```python title="Python"
   data_source = gx.data_sources.add_spark_abs(
      name=data_source_name,
      azure_options=azure_options
   )
   ```

   </TabItem>

   </Tabs>

3. Optional. Retrieve your Data Source from your Data Context.

   You can retrieve your Data Source elsewhere in your code by updating the value of `datasource_name` and executing:

   ```python title="Python"
   datasource_name="nyc_taxi_data"
   datasource = context.data_sources.get(datasource_name)
   ```

   If you are using a File Data Context your Data Source can also be retrieved from the Data Context in future python sessions.

</TabItem>

<TabItem value="sample_code" label="Sample code">

   Choose from the following to see the full example code for a S3 Filesystem Data Source, using either pandas or Spark to read the data files:

   <Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_filesystem" label="pandas example">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   datasource_name = "my_datasource"
   azure_options = {
      "account_url": "${AZURE_STORAGE_ACCOUNT_URL}",
      "credential": "${AZURE_CREDENTIAL}",
   }
   
   # Create the Data Source:
   data_source = gx.data_sources.add_pandas_abs(
      name=data_source_name,
      azure_options=azure_options
   )
   
   # Retrieve the Data Source:
   datasource = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   <TabItem value="spark" label="Spark example">

   ```python title="Python"
   import great_epectations as gx

   context = gx.get_context()

   # Define the Data Source's parameters:
   datasource_name = "my_datasource"
   azure_options = {
      "account_url": "${AZURE_STORAGE_ACCOUNT_URL}",
      "credential": "${AZURE_CREDENTIAL}",
   }
   
   # Create the Data Source:
   data_source = gx.data_sources.add_spark_abs(
      name=data_source_name,
      azure_options=azure_options
   )

   # Retrieve the Data Source:
   datasource = context.data_sources.get("nyc_taxi_data")
   ```

   </TabItem>

   </Tabs>

</TabItem>

</Tabs>