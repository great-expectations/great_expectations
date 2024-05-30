import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

import GxData from '../../../_core_components/_data.jsx'
import PreReqPython from '../../../_core_components/prerequisites/_python_installation.md'
import PreReqGxInstalledWithSqlDependecies from '../../../_core_components/prerequisites/_gx_installation_with_sql_dependencies.md'
import PreReqDataContext from '../../../_core_components/prerequisites/_preconfigured_data_context.md'
import PreReqCredentials from '../../../_core_components/prerequisites/_securely_configured_credentials.md'

import DatasourceMethodReferenceTable from './_datasource_method_reference_table.md'

### Prerequisites
- <PreReqPython/>.
- <PreReqGxInstalledWithSqlDependecies/>
- <PreReqDataContext/>.
- <PreReqCredentials/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import GX and instantiate a Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py imports"
   ```

2. Define a name and connection string for your Data Source.

   You can assign any name to a Data Source as long as it is unique within your Data Context.  You should use a descriptive name to help you remember the Data Sources purpose.

   Your connection string or credentials should not be saved in plain text in your code.  Instead, you should reference a securely stored connection string or credentials through string substitution.  The guidance on how to [Configure your credentials](#configure-your-credentials) covers how to determine the format of your connection string, securely store your connection string or credentials, and how to reference your connection string or credentials in Python.

   The following code defines a Data Source name and references a PostgreSQL connection string that has been securely stored in its entirety:

    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py name and connection string"
   ```

3. Create a Data Source.
   
   {GxData.product_name} provides specific methods for creating Data Sources that correspond to supported SQL dialects.  All of these methods are accessible from the `data_sources` attribute of your Data Context.  Reference the following table to determine the method used for your data's SQL dialect:

   <DatasourceMethodReferenceTable/>

   Once you have the method for your data's SQL dialect, you can call it with the previously defined Data Source name and connection string to create your Data Source.  The following example creates a PostgreSQL Data Source:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py create data source"
   ```

4. Optional. Verify the Data Source is connected:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py verify data source"
   ```
   
   The details of your Data Source are retrieved from the Data Context and displayed.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Sample code" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_source/postgres.py full sample code"
```

</TabItem>

</Tabs>