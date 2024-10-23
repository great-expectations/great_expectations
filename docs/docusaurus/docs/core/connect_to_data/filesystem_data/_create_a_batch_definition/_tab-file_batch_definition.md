import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import GxData from '../../../_core_components/_data.jsx'
import PreReqDataContext from '../../../_core_components/prerequisites/_preconfigured_data_context.md'

Batch Definitions for File Data Assets can be configured to return the content of a specific file based on either a file path or a regex match for dates in the name of the file.

### Prerequisites
- <PreReqDataContext/>.  The variable `context` is used for your Data Context in the following example code.
- [A File Data Asset on a Filesystem Data Source](#create-a-data-asset).

<Tabs>

<TabItem value="instructions" label="Instructions">

1. Retrieve your Data Asset.

   Replace the value of `data_source_name` with the name of your Data Source and the value of `data_asset_name` with the name of your Data Asset in the following code.  Then execute it to retrieve an existing Data Source and Data Asset from your Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - retrieve Data Asset"
   ```

2. Add a Batch Definition to the Data Asset.

   A path Batch Definition returns all of the records in a specific data file as a single Batch.  A partitioned Batch Definition will return the records of a single file in the Data Asset based on which file name matches a regex.

   <Tabs queryString="batch_definition" groupId="batch_definition" defaultValue='path'>

   <TabItem value="path" label="Path">
   
   To define a path Batch Definition you need to provide the following information:

   - `name`: A name by which you can reference the Batch Definition in the future.  This should be unique within the Data Asset.
   - `path`: The path within the Data Asset of the data file containing the records to return.
 
   Update the `batch_definition_name` and `batch_definition_path` variables and execute the following code to add a path Batch Definition to your Data Asset:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="partitioned" label="Partitioned">
   
   GX Core currently supports partitioning File Data Assets based on dates.  The files can be returned by year, month, or day.

   <Tabs queryString="partition_type" groupId="partition_type" defaultValue='yearly'>
   
   <TabItem value="yearly" label="Yearly">
   
   For example, say your Data Asset contains the following files with year dates in the file names:

   - yellow_tripdata_sample_2019.csv
   - yellow_tripdata_sample_2020.csv
   - yellow_tripdata_sample_2021.csv

   You can create a regex that will match these files by replacing the year in the file names with a named regex matching pattern.  This pattern's group should be named `year`.

   For the above three files, the regex pattern would be:

   ```regexp title="Regular Expression"
   yellow_tripdata_sample_(?P<year>\d{4})\.csv
   ```

   Update the `batch_definition_name` and `batch_definition_regex` variables in the following code, then execute it to create a yearly Batch Definition:

   ```python name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="monthly" label="Monthly">
   
   For example, say your Data Asset contains the following files with year and month dates in their names:

   - yellow_tripdata_sample_2019-01.csv
   - yellow_tripdata_sample_2019-02.csv
   - yellow_tripdata_sample_2019-03.csv

   You can create a regex that will match these files by replacing the year and month in the file names with named regex matching patterns.  These patterns should be correspondingly named `year` and `month`.

   For the above three files, the regex pattern would be:

   ```regexp title="Regular Expression"
   yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv
   ```

   Update the `batch_definition_name` and `batch_definition_regex` variables in the following code, then execute it to create a monthly Batch Definition:

   ```python name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - add Batch Definition"
   ```

   </TabItem>

   <TabItem value="daily" label="Daily">
   
   For example, say your Data Asset contains the following files with year, month, and day dates in their names:

   - yellow_tripdata_sample_2019-01-15.csv
   - yellow_tripdata_sample_2019-01-16.csv
   - yellow_tripdata_sample_2019-01-17.csv

   You can create a regex that will match these files by replacing the year, month, and day in the file names with named regex matching patterns.  These patterns should be correspondingly named `year`, `month`, and `day`.

   For the above three files, the regex pattern would be:

   ```regexp title="Regular Expression"
   yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv
   ```

   Update the `batch_definition_name` and `batch_definition_regex` variables in the following code, then execute it to create a daily Batch Definition:

   ```python name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - add Batch Definition"
   ```

   </TabItem>

   </Tabs>

   </TabItem>

   </Tabs>
   
4. Optional. Verify the Batch Definition is valid.
   

   <Tabs className="hidden" queryString="batch_definition" groupId="batch_definition" defaultValue='path'>

   <TabItem value="path" label="Path">

   A path Batch Definition always returns all records in a specific file as a single Batch.  Therefore you do not need to provide any additional parameters to retrieve data from a path Batch Definition.
   
   After retrieving your data you can verify that the Batch Definition is valid by printing the first few retrieved records with `batch.head()`:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - retrieve Batch and verify"
   ```

   </TabItem>

   <TabItem value="partitioned" label="Partitioned">

   When retrieving a Batch from a partitioned Batch Definition, you can specify the date of the data to retrieve by providing a `batch_parameters` dictionary with keys that correspond to the regex matching groups in the Batch Definition.  If you do not specify a date, the most recent date in the data is returned by default.

   After retrieving your data you can verify that the Batch Definition is valid by printing the first few retrieved records with `batch.head()`:

   <Tabs queryString="partition_type" groupId="partition_type" defaultValue='yearly'>
   
   <TabItem value="yearly" label="Yearly">

    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - retrieve Batch and verify"
   ```

   </TabItem>

   <TabItem value="monthly" label="Monthly">

    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - retrieve Batch and verify"
   ```

   </TabItem>

   <TabItem value="daily" label="Daily">
  
    ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - retrieve Batch and verify"
   ```

   </TabItem>

   </Tabs>
  
   </TabItem>

   </Tabs>

5. Optional. Create additional Batch Definitions.

   A Data Asset can have multiple Batch Definitions as long as each Batch Definition has a unique name within that Data Asset. Repeat this procedure to add additional path or partitioned Batch Definitions to your Data Asset.

</TabItem>

<TabItem value="sample_code" label="Sample code">

Full example code for path Batch Definitions and partitioned yearly, monthly, or daily Batch Definitions:

<Tabs queryString="batch" groupId="batch" defaultValue='path'>

<TabItem value="path" label="Path">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_path.py - full_example"
```

</TabItem>

<TabItem value="yearly" label="Yearly">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_yearly.py - full_example"
```

</TabItem>

<TabItem value="monthly" label="Monthly">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - full_example"
```

</TabItem>

<TabItem value="daily" label="Daily">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - full_example"
```

</TabItem>

</Tabs>

</TabItem>

</Tabs>