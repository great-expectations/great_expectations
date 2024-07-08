---
title: Connect to Filesystem data
description: Connect to data stored as files in a folder hierarchy and organize that data into Batches for retrieval and validation.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';
import CreateADataSource from './_create_a_data_source/_create_a_data_source.md';

Filesystem data consists of data stored in file formats such as `.csv` or `.parquet`, and located in an environment with a folder hierarchy such as Amazon S3, Azure Blob Storage, Google Cloud Storage, or local and networked filesystems.

To connect to your Filesystem data, you first create a Data Source which tells GX where your data files reside.  You then configure Data Assets for your Data Source to tell GX which sets of records you want to be able to access from your Data Source.  Finally, you will define Batch Definitions which allow you to request all the records retrieved from a Data Asset or further partition the returned records based on the contents of a date and time field.

## Create a Data Source

<CreateADataSource/>

## Create a Data Asset

A Data Asset is a collection of related records in a Data Source.  

<Tabs className="hidden" queryString="data_location" groupId="data_location" defaultValue="filesystem">

   <TabItem value="filesystem" label="Local or networked filesystem">

   </TabItem>

   <TabItem value="s3" label="Amazon S3">

   </TabItem>

   <TabItem value="abs" label="Azure Blob Storage">

   </TabItem>

   <TabItem value="gcs" label="Google Cloud Storage">

   </TabItem>

</Tabs>

## Create a Batch Definition

A Batch Definition determines which records in a Data Asset are retrieved for Validation.  Batch Definitions can be configured to either provide all of the records in a Data Asset, or to subdivide the Data Asset based on a Datetime field and return Batches based on a requested year, month, or day.

<Tabs className="hidden" queryString="data_location" groupId="data_location" defaultValue="filesystem">

   <TabItem value="filesystem" label="Local or networked filesystem">

   </TabItem>

   <TabItem value="s3" label="Amazon S3">

   </TabItem>

   <TabItem value="abs" label="Azure Blob Storage">

   </TabItem>

   <TabItem value="gcs" label="Google Cloud Storage">

   </TabItem>

</Tabs>

## Retrieve data