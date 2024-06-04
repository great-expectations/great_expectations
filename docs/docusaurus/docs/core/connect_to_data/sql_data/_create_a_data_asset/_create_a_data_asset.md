import PrereqDataSource from '../../../_core_components/prerequisites/_data_source_sql.md'
import PreReqDataContext from '../../../_core_components/prerequisites/_preconfigured_data_context.md'

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import TableAsset from './_table_asset.md';
import QueryAsset from './_query_asset.md';

Data Assets are collections of records within a Data Source.  With SQL Data Sources, a Data Asset can consist of the records from a specific table or the records from a specified query.

### Prerequisites
- <PreReqDataContext/>.  The variable `context` is used for your Data Context in the following example code.
- <PrereqDataSource/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve your Data Source.

   Replace the value of `data_source_name` with the name of your Data Source and execute the following code to retrieve an existing Data Source from your Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py retrieve data source"
   ```

3. Add a Data Asset to your Data Source.

   <Tabs queryString="asset_type" groupId="asset_type" defaultValue='table' values={[{label: 'Table Data Asset', value:'table'}, {label: 'Query Data Asset', value:'query'}]}>

   <TabItem value="table" label="Add a Table Asset">
   <TableAsset/>
   </TabItem>

   <TabItem value="query" label="Add a Query Asset">
   <QueryAsset/>
   </TabItem>

   </Tabs>

4. Optional. Verify that your Data Asset was added to your Data Source:

   ```python
   print(data_source.assets)
   ```
   
   A list of Data Assets is printed.  You can verify your Data Asset was created and added to the Data Source by checking for its name in the printed list.

5. Optional. Add additional Data Assets to your Data Source.

   A Data Source can have multiple Data Assets.  Repeat this procedure to add additional Table or Query Data Assets to your Data Source.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py full code"
```

</TabItem>

</Tabs>