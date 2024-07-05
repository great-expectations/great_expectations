---
title: Connect to Filesystem data
description: Connect to data stored as files in a folder hierarchy and organize that data into Batches for retrieval and validation.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Filesystem data consists of data stored in file formats such as `.csv` or `.parquet`, and located in an environment with a folder hierarchy such as Amazon S3, Azure Blob Storage, Google Cloud Storage, or local and networked filesystems.

To connect to your Filesystem data, you first create a Data Source which tells GX where your data files reside.  You then configure Data Assets for your Data Source to tell GX which sets of records you want to be able to access from your Data Source.  Finally, you will define Batch Definitions which allow you to request all the records retrieved from a Data Asset or further partition the returned records based on the contents of a date and time field.

## Create a Data Source

Data Sources tell GX where your data is located and how to connect to it.  With Filesystem data this is done by directing GX to the folder that contains the data files.  GX supports accessing Filesystem data from Amazon S3, Azure Blob Storage, Google Cloud Storage, and local or networked filesystems.

<Tabs queryString="data_source_type" groupId="data_source_type" defaultValue="file" values={[{label: "File Data Source", value:"file"}, {label: "Directory Data Source", value:"directory"}]}>

   <TabItem value="file" label="File Data Source">

   </TabItem>

   <TabItem value="directory" label="Directory Data Source">

   </TabItem>

</Tabs>

## Create a Data Asset

A Data Asset is a collection of related records in a Data Source.  

## Create a Batch Definition

A Batch Definition determines which records in a Data Asset are retrieved for Validation.  Batch Definitions can be configured to either provide all of the records in a Data Asset, or to subdivide the Data Asset based on a Datetime field and return Batches based on a requested year, month, or day.
