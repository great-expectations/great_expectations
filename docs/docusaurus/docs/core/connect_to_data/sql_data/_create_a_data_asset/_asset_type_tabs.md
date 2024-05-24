import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import TableAsset from './_table_asset.md';
import QueryAsset from './_query_asset.md';

## Prerequisites
- A Data Source that connects to a SQL database.
- A Data Context (`gx.get_context()`).

### Procedure.

1. Retrieve your Data Source.

   This can be a Data Source objecy that you have already defined in your code (such as a newly created Data Source) or a pre-existing Data Source that you retrieve from your Data Context.

   To retrieve a Data Source from your Data Context, change the value of `data_source_name` in the following code to the name of your Data Source and then run the code.

   ```python
   data_source_name = "NAME_OF_MY_DATA_SOURCE"
   data_source = context.data_sources.get(data_source_name)
   ```

2. Add a Data Asset to your Data Source.

   <Tabs queryString="asset_type" groupId="asset_type" defaultValue='table' values={[{label: 'Table', value:'table'}, {label: 'Query', value:'query'}]}>

   <TabItem value="table" label="Table">
   <TableAsset/>
   </TabItem>

   <TabItem value="query" label="Query">
   <QueryAsset/>
   </TabItem>

   </Tabs>

3. Optional. Verify that your Data Asset was added to your Data Source.

   ```python
   print(data_source.assets)
   ```

4. Optional. Add additional Data Assets to your Data Source.

   A Data Source can have multiple Data Assets.  Repeat this procedure to add additional Table or Query Data Assets to your Data Source.