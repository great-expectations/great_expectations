import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import LocalOrNetworkedDataAsset from './_local_or_networked/_local_or_networked.md';
import AmazonS3DataAsset from './_s3/_s3.md';
import AzureBlobStorageDataAsset from './_abs/_abs.md';
import GoogleCloudStorageDataAsset from './_gcs/_gcs.md';

A Data Asset is a collection of related records within a Data Source.  These records may be located within multiple files, but each Data Asset is only capable of reading a single specific file format which is determined when it is created.  However, a Data Source may contain multiple Data Assets covering different file formats and groups of records.

GX provides two types of Data Assets for Filesystem Data Sources: File Data Assets and Directory Data Assets.

<Tabs queryString="data_asset" groupId="data_asset" defaultValue='file'>

   <TabItem value="file" label="File Data Asset">

   File Data Assets are used to retrieve data from individual files in formats such as `.csv` or `.parquet`.  The file format that can be read by a File Data Asset is determined when the File Data Asset is created.  The specific file that is read is determind by Batch Definitions that are added to the Data Asset after it is created.

   Both Spark and pandas Filesystem Data Sources support File Data Assets for all supported Filesystem environments.

   </TabItem>

   <TabItem value="directory" label="Directory Data Asset">

   Directory Data Assets read one or more files in formats such as `.csv` or `.parquet`.  The file format that can be read by a Directory Data Asset is determined when the Directory Data Asset is created.  The data in the corresponding files is concatonated into a single table which can be retrieved as a whole, or further partitioned based on the value of a datetime field.

   Spark Filesystem Data Sources support Directory Data Assets for all supported Filesystem environments.  However, pandas Filesystem Data Sources do _not_ support Directory Data Assets at all.

   </TabItem>

</Tabs>

<Tabs queryString="environment" groupId="environment" defaultValue="filesystem">

   <TabItem value="filesystem" label="Local or networked filesystem">
   <LocalOrNetworkedDataAsset/>
   </TabItem>

   <TabItem value="s3" label="Amazon S3">
   <AmazonS3DataAsset/>
   </TabItem>

   <TabItem value="abs" label="Azure Blob Storage">
   <AzureBlobStorageDataAsset/>
   </TabItem>

   <TabItem value="gcs" label="Google Cloud Storage">
   <GoogleCloudStorageDataAsset/>
   </TabItem>

</Tabs>
