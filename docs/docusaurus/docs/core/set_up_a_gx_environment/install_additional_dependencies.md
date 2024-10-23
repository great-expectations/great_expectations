---
title: Install additional dependencies
hide_table_of_contents: true
---
import GxData from '../_core_components/_data.jsx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import InstallAmazon from './_install_additional_dependencies/_amazon_s3.md'
import InstallAzureBlobStorage from './_install_additional_dependencies/_azure_blob_storage.md'
import InstallGoogleCloudPlatform from './_install_additional_dependencies/_google_cloud_platform.md'
import InstallSql from './_install_additional_dependencies/_sql.md'
import InstallSpark from './_install_additional_dependencies/_spark.md'

Some environments and Data Sources require additional Python libraries or third party utilities that are not included in the base installation of
GX Core. Use the information provided here to install the necessary dependencies for your databases.

<Tabs queryString="dependencies" groupId="additional-dependencies" defaultValue='amazon' values={[{value: 'amazon', label: 'Amazon S3'}, {label: 'Microsoft Azure Blob Storage', value:'azure'}, {label: 'Google Cloud Storage', value:'gcs'}, {label: 'SQL databases', value:'sql'}, {label: 'Spark', value:'spark'}]}>

  <TabItem value="amazon" label="Amazon S3">
<InstallAmazon/>
  </TabItem>

  <TabItem value="azure">
<InstallAzureBlobStorage/>
  </TabItem>

  <TabItem value="gcs">
<InstallGoogleCloudPlatform/>
  </TabItem>

  <TabItem value="sql">
<InstallSql/>
  </TabItem>

  <TabItem value="spark">
<InstallSpark/>
  </TabItem>

</Tabs>
