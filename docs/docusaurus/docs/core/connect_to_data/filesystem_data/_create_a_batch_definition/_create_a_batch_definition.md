import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import TabFileDataAsset from './_tab-file_batch_definition.md';
import TabDirectoryDataAsset from './_tab-directory_batch_definition.md';

A Batch Definition determines which records in a Data Asset are retrieved for Validation.  Batch Definitions can be configured to either provide all of the records in a Data Asset, or to subdivide the Data Asset based on a date.

<Tabs queryString="data_asset" groupId="data_asset" defaultValue='file'>

   <TabItem value="file" label="File Data Asset">
   <TabFileDataAsset/>
   </TabItem>

   <TabItem value="directory" label="Directory Data Asset">
   <TabDirectoryDataAsset/>
   </TabItem>

</Tabs>