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
import S3ExampleCodePandas from './_amazon_s3/_full_example_code_pandas.md'
import S3ExampleCodeSpark from './_amazon_s3/_full_example_code_spark.md'
import S3NextSteps from './_amazon_s3/_next_steps.md'

import AzurePrerequisites from './_azure_blob_storage/_prerequisites.md'
import AzureDataSourcePandas from './_azure_blob_storage/_add_data_source_pandas.md'
import AzureDataSourceSpark from './_azure_blob_storage/_add_data_source_spark.md'
import AzureDataAssetPandas from './_azure_blob_storage/_add_data_asset_pandas.md'
import AzureDataAssetSpark from './_azure_blob_storage/_add_data_asset_spark.md'
import AzureExampleCodePandas from './_azure_blob_storage/_full_example_code_pandas.md'
import AzureExampleCodeSpark from './_azure_blob_storage/_full_example_code_spark.md'
import AzureNextSteps from './_azure_blob_storage/_next_steps.md'

import FilesystemPrerequisites from './_filesystem/_prerequisites.md'
import FilesystemDataSourcePandas from './_filesystem/_add_data_source_pandas.md'
import FilesystemDataSourceSpark from './_filesystem/_add_data_source_spark.md'
import FilesystemDataAssetPandas from './_filesystem/_add_data_asset_pandas.md'
import FilesystemDataAssetSpark from './_filesystem/_add_data_asset_spark.md'
import FilesystemExampleCodePandas from './_filesystem/_full_example_code_pandas.md'
import FilesystemExampleCodeSpark from './_filesystem/_full_example_code_spark.md'
import FilesystemNextSteps from './_filesystem/_next_steps.md'

import GcsPrerequisites from './_google_cloud_storage/_prerequisites.md'
import GcsDataSourcePandas from './_google_cloud_storage/_add_data_source_pandas.md'
import GcsDataSourceSpark from './_google_cloud_storage/_add_data_source_spark.md'
import GcsDataAssetPandas from './_google_cloud_storage/_add_data_asset_pandas.md'
import GcsDataAssetSpark from './_google_cloud_storage/_add_data_asset_spark.md'
import GcsExampleCodePandas from './_google_cloud_storage/_full_example_code_pandas.md'
import GcsExampleCodeSpark from './_google_cloud_storage/_full_example_code_spark.md'
import GcsNextSteps from './_google_cloud_storage/_next_steps.md'

Use the information provided here to connect to data stored as CSV or similar file formats.

Select the storage location of your data files from the following:

<Tabs queryString="data-source" groupId="connect-filesystem-source-data" defaultValue='amazon'>
  <TabItem value="amazon" label="Amazon S3">

Create a Data Source that tells GX how to connect to your Amazon S3 bucket and a Data Asset that tells GX which files contain the data to make available.

  </TabItem>
  <TabItem value="azure" label="Microsoft Azure Blob Storage">

Create a Data Source that tells GX how to connect to your Microsoft Azure Blob Storage account and a Data Asset that tells GX which files contain the data to make available.

  </TabItem>
  <TabItem value="gcs" label="Google Cloud Storage">

Create a Data Source that tells GX how to connect to your GCS bucket and a Data Asset that tells GX which files contain the data to make available.

  </TabItem>
  <TabItem value="filesystem" label="Filesystem">

Create a Data Source that tells GX where to find your local or networked data files and a Data Asset that tells GX which files contain the data to make available.

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

## (Optional) Add additional Data Assets

A Data Source can contain multiple Data Assets.  If you have additional files to connect to, you can provide different `name` and `batching_regex` parameters to create additional Data Assets for those files in your Data Source.  You can even include the same files in multiple Data Assets: If a given file matches the `batching_regex` of more than one Data Asset it will be included in each Data Asset it matches with.

## Full example code

<Tabs className="hidden" groupId="connect-filesystem-source-data" defaultValue='amazon'>
  <TabItem value="amazon">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <S3ExampleCodePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <S3ExampleCodeSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="azure">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <AzureExampleCodePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <AzureExampleCodeSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="gcs">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <GcsExampleCodePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <GcsExampleCodeSpark/>
      </TabItem>
    </Tabs>
  </TabItem>

  <TabItem value="filesystem">
    <Tabs className="hidden" queryString="data-connector" groupId="data-connector" defaultValue='pandas'>
      <TabItem value="pandas" label="Using pandas">
        <FilesystemExampleCodePandas/>
      </TabItem>
      <TabItem value="spark" label="Using Spark">
        <FilesystemExampleCodeSpark/>
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