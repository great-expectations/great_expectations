import PrereqDataSource from '../../../_core_components/prerequisites/_data_source_sql.md'

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import TableAsset from './_table_asset.md';
import QueryAsset from './_query_asset.md';

Data Assets are collections of records within a Data Source.  With SQL Data Sources, a Data Asset can consist of the records from a specific table or the records from a specified query.

### Prerequisites
- <PrereqDataSource/>.  The variable `data_source` is used for your Data Source in the following example code.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Add a Data Asset to your Data Source.

   <Tabs queryString="asset_type" groupId="asset_type" defaultValue='table' values={[{label: 'Table Data Asset', value:'table'}, {label: 'Query Data Asset', value:'query'}]}>

   <TabItem value="table" label="Add a Table Asset">
   <TableAsset/>
   </TabItem>

   <TabItem value="query" label="Add a Query Asset">
   <QueryAsset/>
   </TabItem>

   </Tabs>

2. Optional. Verify that your Data Asset was added to your Data Source:

   ```python
   print(data_source.assets)
   ```
   
   A list of Data Assets is printed.  You can verify your Data Asset was created and added to the Data Source by checking for its name in the printed list.

3. Optional. Add additional Data Assets to your Data Source.

   A Data Source can have multiple Data Assets.  Repeat this procedure to add additional Table or Query Data Assets to your Data Source.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Full sample code" name="docs/docusaurus/docs/core/connect_to_data/sql_data/_create_a_data_asset/create_a_data_asset.py full code"
```

</TabItem>

</Tabs>