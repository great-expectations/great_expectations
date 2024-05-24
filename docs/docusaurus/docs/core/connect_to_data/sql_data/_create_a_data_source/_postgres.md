import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

### Prerequisites
- Python
- GX installed
- PostgreSQL dependencies installed

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Import GX and instantiate a Data Context:

   ```python
   import great_expectations as gx
   context = gx.get_context()
   ```

2. Get your connection string.

   A PostgreSQL connection string takes the format:

   ```text title="PostgreSQL connection string format"
   postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>
   ```

   Replace the text in `<>` with your corresponding credentials.  If you set your credentials as environment variables, or stored them in `credentials.yml` in a File Data Context, you can reference those variables instead.  For example:

   ```python title="Python"
   my_connection_string = "postgresql+psycopg2://${USERNAME}:${PASSWORD}@<host>:<port>/<database>"
   ```
   
   or, if you stored your entire connection string in a single variable:
   
   ```python title="Python"
   my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"
   ```
   
3. Create a PostgreSQL Data Source.

   The name of your Data Source should be unique and descriptive.  Change the value of `datasource_name` in the following code and then execute it to create a PostgreSQL Data Source:

   ```python title="Python"
   datasource_name = "my_sql_data_source"
   datasource = context.sources.add_postgres(name=datasource_name, connection_string=my_connection_string)
   ```

4. Optional. Verify the Data Source is connected:

   ```python
   print(context.datasources)
   ```
   
   A list of Data Sources is printed.  You can verify your Data Source was created and added to the Data Context by checking for its name in the printed list.

5. Create a Data Asset:

   <Tabs queryString="asset_type" groupId="asset_type" defaultValue='table' values={[{label: 'Table Asset', value:'table'}, {label: 'Query Asset', value:'query'}]}>
   
   <TabItem value="table" label="Table Asset">
   ```python
   data_asset = data_source.add_table_asset()
   ```
   </TabItem>

   <TabItem value="query" label="Query Asset">
   ```python
   data_asset = data_source.add_table_asset()
   ```
   </TabItem>

   </Tabs>
   
   
6. Optional. Create additional Data Assets.


7. Create a Batch Definition.

   <Tabs queryString="batch_definition" groupId="batch_definition" defaultValue='whole_table' values={[{label: 'Whole table', value:'whole_table'}, {label: 'Partitioned', value:'partitioned'}]}>
   
   <TabItem value="whole_table" label="Whole table">
   ```python
   batch_definition = data_source.add_batch_definition_whole_table()
   ```
   </TabItem>

   <TabItem value="partitioned" label="Partitioned">
   ```python
   daily_batch_definition = data_source.add_batch_definition_daily()
   monthly_batch_definition = data_source.add_batch_definition_monthly()
   yearly_batch_definition = data_source.add_batch_definition_yearly()
   ```
   </TabItem>

   </Tabs>

8. Optional. Verify the Batch Definition is valid.

   ```python
   batch = batch_definition.get_batch()
   batch.head()
   ```

9. Optional. Create additional Batch Definitions.


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