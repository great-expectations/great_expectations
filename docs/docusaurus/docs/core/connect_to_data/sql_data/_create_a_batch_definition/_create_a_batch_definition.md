import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve your Data Asset

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py retrieve a Data Asset"
   ```

2. Add a Batch Definition to the Data Asset

   <Tabs queryString="batch_definition" groupId="batch_definition" defaultValue='whole_table' values={[{label: 'Whole table', value:'whole_table'}, {label: 'Partitioned', value:'partitioned'}]}>

   <TabItem value="whole_table" label="Whole table">
   
   A full table Batch Definition returns all of the records in your Data Asset as a single Batch.  Therefore, to define a full table Batch Definition you only need to provide a name for the Batch Definition to be referenced by.
 
   Update the `name` parameter and execute the following code to create a full table Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md full table batch definition"
   ```
   </TabItem>

   <TabItem value="partitioned" label="Partitioned">
   
   A partitioned Batch Definition subdivides the records in a Data Asset based on the values in a specified field.  {Gx.product_name} currently supports partitioning Data Assets based on date fields.  The records can be grouped by year, month, or day.

   Update the `date_column` variable and `name` parameters in the following snippet, then execute it to create partitioned Batch Definitions:

   ```python name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md daily batch definition"
   ```
   </TabItem>

   </Tabs>

5. Optional. Verify the Batch Definition is valid.

   When retrieving a Batch from a partitioned Batch Definition, you can specify the date of the data to retrieve as shown in the following examples.  If you do not specify a date, the most recent date in the data is returned by default.

   <Tabs queryString="batch_definition" groupId="batch_definition" defaultValue='whole_table' values={[{label: 'Whole table', value:'whole_table'}, {label: 'Partitioned', value:'partitioned'}]}>

   <TabItem value="whole_table" label="Whole table">
   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md verify full table"
   ```
   </TabItem>

   <TabItem value="partitioned" label="Partitioned">
   ```python name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md verify daily"
   ```
   </TabItem>

   </Tabs>

6. Optional. Create additional Batch Definitions.

   A Data Asset can have multiple Batch Definitions as long as each Batch Definition has a unique name within that Data Asset. Repeat this procedure to add additional full table or partitioned Batch Definitions to your Data Asset.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_batch_definition/_create_a_batch_definition.md full example"
```

</TabItem>

</Tabs>