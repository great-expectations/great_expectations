import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import LocalOrNetworkedDataSource from './_local_or_networked/_local_or_networked.md';
import AmazonS3DataSource from './_s3/_s3.md';
import AzureBlobStorageDataSource from './_abs/_abs.md';
import GoogleCloudStorageDataSource from './_gcs/_gcs.md';

Data Sources tell GX where your data is located and how to connect to it.  With Filesystem data this is done by directing GX to the folder or online location that contains the data files.  GX supports accessing Filesystem data from Amazon S3, Azure Blob Storage, Google Cloud Storage, and local or networked filesystems.

<Tabs queryString="environment" groupId="environment" defaultValue="filesystem">

   <TabItem value="filesystem" label="Local or networked filesystem">
   <LocalOrNetworkedDataSource/>
   </TabItem>

   <TabItem value="s3" label="Amazon S3">
   <AmazonS3DataSource/>
   </TabItem>

   <TabItem value="abs" label="Azure Blob Storage">
   <AzureBlobStorageDataSource/>
   </TabItem>

   <TabItem value="gcs" label="Google Cloud Storage">
   <GoogleCloudStorageDataSource/>
   </TabItem>

</Tabs>
