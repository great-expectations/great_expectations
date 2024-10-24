import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import GxData from '../../../_core_components/_data.jsx'
import PreReqDataContext from '../../../_core_components/prerequisites/_preconfigured_data_context.md'

Batch Definitions for a Directory Data Asset can be configured to return all of the records for the files in the Data Asset, or to subdivide the Data Asset's records on the content of a Datetime field and only return the records that correspond to a specific year, month, or day. 

### Prerequisites
- <PreReqDataContext/>.  The variable `context` is used for your Data Context in the following example code.
- [A File Data Asset on a Filesystem Data Source](#create-a-data-asset).

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Retrieve your Data Asset.

   Replace the value of `data_source_name` with the name of your Data Source and the value of `data_asset_name` with the name of your Data Asset in the following code.  Then execute it to retrieve an existing Data Source and Data Asset from your Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve Data Asset"
   ```

2. Add a Batch Definition to the Data Asset.

   A whole directory Batch Definition returns all of the records in a data file as a single Batch.  A partitioned directory Batch Definition will subdivide the records according to a datetime field and return those records that match a specified year, month, or day.

   <Tabs queryString="batch_definition" groupId="batch_definition" defaultValue='whole_directory'>

   <TabItem value="whole_directory" label="Whole directory">
   
   Because a whole directory Batch Definition returns the records from all of the files it can read in the Data Asset you only need to provide one addditional piece of information to define one:

   - `name`: A name by which you can reference the Batch Definition in the future.  This should be unique within the Data Asset.
 
   Update the `batch_definition_name` variable and execute the following code to create a whole directory Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="partitioned" label="Partitioned">
   
   GX Core currently supports partitioning Directory Data Assets based on a datetime field.  Therefore, to define a partitioned Directory Batch Definition you need to provide two pieces of information:

   - `name`:A name by which you can reference the Batch Definition in the future.  This should be unique within the Data Asset.
   - `column`: The datetime column that records should be subdivided on.

   The Batch Definition can be configured to return records by year, month, or day.

   <Tabs queryString="partition_type" groupId="partition_type" defaultValue='yearly'>
   
   <TabItem value="yearly" label="Yearly">

   Update the `batch_definition_name` and `batch_definition_column` variables in the following code, then execute it to create a Batch Definition that subdivides the records in a directory by year:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_yearly.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="monthly" label="Monthly">
   
   Update the `batch_definition_name` and `batch_definition_column` variables in the following code, then execute it to create a Batch Definition that subdivides the records in a directory by month:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="daily" label="Daily">
   
   Update the `batch_definition_name` and `batch_definition_column` variables in the following code, then execute it to create a Batch Definition that subdivides the records in a directory by day:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_daily.py - add Batch Definition"
   ```

   </TabItem>

   </Tabs>

   </TabItem>

   </Tabs>
   
5. Optional. Verify the Batch Definition is valid.

   <Tabs className="hidden" queryString="batch_definition" groupId="batch_definition" defaultValue='whole_directory'>

   <TabItem value="whole_directory" label="Whole directory">

   A whole directory Batch Definition always returns all available records as a single Batch.  Therefore you do not need to provide any additional parameters to retrieve data from a path Batch Definition.
   
   After retrieving your data you can verify that the Batch Definition is valid by printing the first few retrieved records with `batch.head()`:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - retrieve and verify Batch"
   ```

   </TabItem>

   <TabItem value="partitioned" label="Partitioned">

   When retrieving a Batch from a partitioned Batch Definition, you can specify the date of the data to retrieve by providing a `batch_parameters` dictionary with keys that correspond to the regex groups in the Batch Definition.  If you do not specify a date, the most recent date in the data is returned by default.

   After retrieving your data you can verify that the Batch Definition is valid by printing the first few retrieved records with `batch.head()`:

   <Tabs queryString="partition_type" groupId="partition_type" defaultValue='yearly'>
   
   <TabItem value="yearly" label="Yearly">

    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_yearly.py - retrieve and verify Batch"
   ```

   </TabItem>

   <TabItem value="monthly" label="Monthly">

    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - retrieve and verify Batch"
   ```

   </TabItem>

   <TabItem value="daily" label="Daily">
  
    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_daily.py - retrieve and verify Batch"
   ```

   </TabItem>

   </Tabs>
  
   </TabItem>

   </Tabs>

5. Optional. Create additional Batch Definitions.

   A Data Asset can have multiple Batch Definitions as long as each Batch Definition has a unique name within that Data Asset. Repeat this procedure to add additional whole directory or partitioned Batch Definitions to your Data Asset.

</TabItem>

<TabItem value="sample_code" label="Sample code">

Full example code for whole directory Batch Definitions and partitioned yearly, monthly, or daily Batch Definitions:

<Tabs queryString="batch" groupId="batch" defaultValue='whole_directory'>

<TabItem value="whole_directory" label="Whole directory">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_whole_directory.py - full_example"
```

</TabItem>

<TabItem value="yearly" label="Yearly">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_yearly.py - full example"
```

</TabItem>

<TabItem value="monthly" label="Monthly">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_monthly.py - full example"
```

</TabItem>

<TabItem value="daily" label="Daily">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_directory_partitioned_daily.py - full example"
```

</TabItem>

</Tabs>

</TabItem>

</Tabs>