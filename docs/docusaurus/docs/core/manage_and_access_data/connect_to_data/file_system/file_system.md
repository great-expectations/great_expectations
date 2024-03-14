---
title: Connect to file system data
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import S3Prerequisites from './_amazon_s3/_prerequisites.md'
import S3DataSourcePandas from './_amazon_s3/_add_data_source_pandas.md'
import S3DataSourceSpark from './_amazon_s3/_add_data_source_spark.md'
import S3DataAssetPandas from './_amazon_s3/_add_data_asset_pandas.md'
import S3DataAssetSpark from './_amazon_s3/_add_data_asset_spark.md'
import S3NextSteps from './_amazon_s3/_next_steps.md'

import AzurePrerequisites from './_azure_blob_storage/_prerequisites.md'
import AzureDataSourcePandas from './_azure_blob_storage/_add_data_source_pandas.md'
import AzureDataSourceSpark from './_azure_blob_storage/_add_data_source_spark.md'
import AzureDataAssetPandas from './_azure_blob_storage/_add_data_asset_pandas.md'
import AzureDataAssetSpark from './_azure_blob_storage/_add_data_asset_spark.md'
import AzureNextSteps from './_azure_blob_storage/_next_steps.md'

import FilesystemPrerequisites from './_filesystem/_prerequisites.md'
import FilesystemDataSourcePandas from './_filesystem/_add_data_source_pandas.md'
import FilesystemDataSourceSpark from './_filesystem/_add_data_source_spark.md'
import FilesystemDataAssetPandas from './_filesystem/_add_data_asset_pandas.md'
import FilesystemDataAssetSpark from './_filesystem/_add_data_asset_spark.md'
import FilesystemNextSteps from './_filesystem/_next_steps.md'

import GcsPrerequisites from './_google_cloud_storage/_prerequisites.md'
import GcsDataSourcePandas from './_google_cloud_storage/_add_data_source_pandas.md'
import GcsDataSourceSpark from './_google_cloud_storage/_add_data_source_spark.md'
import GcsDataAssetPandas from './_google_cloud_storage/_add_data_asset_pandas.md'
import GcsDataAssetSpark from './_google_cloud_storage/_add_data_asset_spark.md'
import GcsNextSteps from './_google_cloud_storage/_next_steps.md'

Use the information provided here to connect to Data Assets stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems.

<Tabs queryString="data-source" groupId="connect-filesystem-source-data" defaultValue='amazon'>
  <TabItem value="amazon" label="Amazon S3">

<h2>Amazon S3 Data Source</h2>

Connect to an Amazon S3 Data Source.

  </TabItem>
  <TabItem value="azure" label="Microsoft Azure Blob Storage">

<h2>Microsoft Azure Blob Storage</h2>

Connect to a Microsoft Azure Blob Storage Data Source.

  </TabItem>
  <TabItem value="gcs" label="Google Cloud Storage">

<h2>GCS Data Source</h2>

Connect to a GCS Data Source.

  </TabItem>
  <TabItem value="filesystem" label="Filesystem">

<h2>Filesystem Data Source</h2>

Connect to a data stored in a filesystem.

  </TabItem>

</Tabs>

## Prerequisites

<Tabs className="hidden" groupId="connect-filesystem-source-data" defaultValue='amazon'>

  <TabItem value="amazon" label="Amazon S3">
    <S3Prerequisites/>
  </TabItem>

  <TabItem value="azure" label="Microsoft Azure Blob Storage">
    <AzurePrerequisites/>
  </TabItem>

  <TabItem value="gcs" label="Google Cloud Storage">
    <GcsPrerequisites/>
  </TabItem>

  <TabItem value="filesystem" label="File system">
    <FilesystemPrerequisites/>
  </TabItem>

</Tabs>

## Create a Data Source

A Data Source tells GX where your data is stored and how to access it.

<Tabs className="hidden" groupId="connect-filesystem-source-data" defaultValue='amazon'>
  <TabItem value="amazon">
    <Tabs queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <S3DataSourcePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <S3DataSourceSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="azure">
    <Tabs queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <AzureDataSourcePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <AzureDataSourceSpark/>
      </TabItem>
    </Tabs>
  </TabItem>
  
  <TabItem value="gcs">
    <Tabs queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <GcsDataSourcePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <GcsDataSourceSpark/>
      </TabItem>
    </Tabs>
  </TabItem>
  
  <TabItem value="filesystem">
    <Tabs queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <FilesystemDataSourcePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <FilesystemDataSourceSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

</Tabs>

## Add a Data Asset to a Data Source

A Data Asset tells GX what set of records to make available from a Data Source.

<Tabs className="hidden" groupId="connect-filesystem-source-data" defaultValue='amazon'>
  <TabItem value="amazon">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <S3DataAssetPandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <S3DataAssetSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="azure">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <AzureDataAssetPandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <AzureDataAssetSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="gcs">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <GcsDataAssetPandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <GcsDataAssetSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="filesystem">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <FilesystemDataAssetPandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <FilesystemDataAssetSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

</Tabs>

## Next steps

<Tabs className="hidden" groupId="connect-filesystem-source-data" defaultValue='amazon'>
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