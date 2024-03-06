---
title: Connect to file system data
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import S3Prerequisites from './_amazon_s3/_prerequisites.md'
import S3DataSource from './_amazon_s3/_add_data_source.md'
import S3DataAsset from './_amazon_s3/_add_data_asset.md'
import S3NextSteps from './_amazon_s3/_next_steps.md'

import AzurePrerequisites from './_azure_blob_storage/_prerequisites.md'
import AzureDataSource from './_azure_blob_storage/_add_data_source.md'
import AzureDataAsset from './_azure_blob_storage/_add_data_asset.md'
import AzureNextSteps from './_azure_blob_storage/_next_steps.md'

import FilesystemPrerequisites from './_filesystem/_prerequisites.md'
import FilesystemDataSource from './_filesystem/_add_data_source.md'
import FilesystemDataAsset from './_filesystem/_add_data_asset.md'
import FilesystemNextSteps from './_filesystem/_next_steps.md'

import GcsPrerequisites from './_google_cloud_storage/_prerequisites.md'
import GcsDataSource from './_google_cloud_storage/_add_data_source.md'
import GcsDataAsset from './_google_cloud_storage/_add_data_asset.md'
import GcsNextSteps from './_google_cloud_storage/_next_steps.md'

Use the information provided here to connect to Data Assets stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems.

<Tabs
  queryString="data-source"
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

  <TabItem value="amazon">

<h2>Amazon S3 Data Source</h2>

Connect to an Amazon S3 Data Source.

  </TabItem>

  <TabItem value="azure">

<h2>Microsoft Azure Blob Storage</h2>

Connect to a Microsoft Azure Blob Storage Data Source.

  </TabItem>

  <TabItem value="gcs">

<h2>GCS Data Source</h2>

Connect to a GCS Data Source.

  </TabItem>

  <TabItem value="filesystem">

<h2>Filesystem Data Source</h2>

Connect to a data stored in a filesystem.

  </TabItem>

</Tabs>

## Prerequisites

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

  <TabItem value="amazon">
<S3Prerequisites/>
  </TabItem>

  <TabItem value="azure">
<AzurePrerequisites/>
  </TabItem>

  <TabItem value="gcs">
<GcsPrerequisites/>
  </TabItem>

  <TabItem value="filesystem">
<FilesystemPrerequisites/>
  </TabItem>

</Tabs>

## Create a Data Source

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

  <TabItem value="amazon">
<S3DataSource/>
  </TabItem>

  <TabItem value="azure">
<AzureDataSource/>
  </TabItem>

  <TabItem value="gcs">
<GcsDataSource/>
  </TabItem>

  <TabItem value="filesystem">
<FilesystemDataSource/>
  </TabItem>

</Tabs>

## Add a Data Asset to a Data Source

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

  <TabItem value="amazon">
<S3DataAsset/>
  </TabItem>

  <TabItem value="azure">
<AzureDataAsset/>
  </TabItem>

  <TabItem value="gcs">
<GcsDataAsset/>
  </TabItem>

  <TabItem value="filesystem">
<FilesystemDataAsset/>
  </TabItem>

</Tabs>

## Next steps

<Tabs
  groupId="connect-filesystem-source-data"
  defaultValue='amazon'
  values={[
  {label: 'Amazon S3', value:'amazon'},
  {label: 'Microsoft Azure Blob Storage', value:'azure'},
  {label: 'Google Cloud Storage', value:'gcs'},
  {label: 'Filesystem', value:'filesystem'},
  ]}>

  <TabItem value="amazon">
<S3NextSteps/>
  </TabItem>

  <TabItem value="azure">
<AzureNextSteps/>
  </TabItem>

  <TabItem value="gcs">
<GcsNextSteps/>
  </TabItem>

  <TabItem value="filesystem">
<FilesystemNextSteps/>
  </TabItem>

</Tabs>