---
title: Connect to Filesystem data
description: Connect to data stored as files in a folder hierarchy and organize that data into Batches for retrieval and validation.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import CreateADataSource from './_create_a_data_source/_create_a_data_source.md';
import CreateADataAsset from './_create_a_data_asset/_create_a_data_asset.md';
import CreateABatchDefinition from './_create_a_batch_definition/_create_a_batch_definition.md';

Filesystem data consists of data stored in file formats such as `.csv` or `.parquet`, and located in an environment with a folder hierarchy such as Amazon S3, Azure Blob Storage, Google Cloud Storage, or local and networked filesystems.  GX can leverage either pandas or Spark to read this data.

To connect to your Filesystem data, you first create a Data Source which tells GX where your data files reside.  You then configure Data Assets for your Data Source to tell GX which sets of records you want to be able to access from your Data Source.  Finally, you will define Batch Definitions which allow you to request all the records retrieved from a Data Asset or further partition the returned records based on a specified date.

## Create a Data Source

<CreateADataSource/>

## Create a Data Asset

<CreateADataAsset/>

## Create a Batch Definition

<CreateABatchDefinition/>

## Retrieve data

Filesystem data can be retrieved from your Batch Definition or through the `.read_*(...)` methods available from the `pandas_default` Data Source.

<Tabs queryString="retrieval_method" groupId="retrieval_method" defaultValue='batch_definition'>

   <TabItem value="batch_definition" label="Batch Definition">
   
   </TabItem>

   <TabItem value="pandas_default" label="pandas_default">
   
   </TabItem>

</Tabs>