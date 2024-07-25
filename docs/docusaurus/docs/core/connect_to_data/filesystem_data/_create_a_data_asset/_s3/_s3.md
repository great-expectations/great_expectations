import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import TabFileDataAsset from './_tab-file_data_asset.md';
import TabDirectoryDataAsset from './_tab-directory_data_asset.md';

<Tabs className="hidden" queryString="data_asset" groupId="data_asset" defaultValue='file'>

   <TabItem value="file" label="File Data Asset">
   <TabFileDataAsset/>
   </TabItem>

   <TabItem value="directory" label="Directory Data Asset">
   <TabDirectoryDataAsset/>
   </TabItem>

</Tabs>