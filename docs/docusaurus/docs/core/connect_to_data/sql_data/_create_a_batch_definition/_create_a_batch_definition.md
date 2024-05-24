import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

1. Retrieve your Data Asset

2. Add a Batch Definition to the Data Asset

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

3. Optional. Verify the Batch Definition is valid.

   ```python
   batch = batch_definition.get_batch()
   batch.head()
   ```

4. Optional. Create additional Batch Definitions.