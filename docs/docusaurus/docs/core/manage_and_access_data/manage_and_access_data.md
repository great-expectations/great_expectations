---
title: Manage and access data
description: Connect to data stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems.
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import InProgress from '../_core_components/_in_progress.md';

<OverviewCard title={frontMatter.title}>
  Connect to Great Expectations (GX) to your data, manage GX's data objects, and retrieve data for use by the GX Python library.
</OverviewCard>

## Access data

Connect GX to data stored on filesystems, databases, or in memory.  Then tell GX how that data should be organized for retrieval and use.

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Connect to filesystem data"
    description="Connect GX to data that is stored as one or more files (such as .csv or .parquet files) in a directory style environment ."
    to="/core/manage_and_access_data/connect_to_data/file_system" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Connect to in-memory data"
    description="Connect GX to data to data that has been read into memory with pandas or Spark."
    to="/core/manage_and_access_data/connect_to_data/in_memory" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Connect to SQL database data"
    description="Connect GX to data stored in SQL databases, with support for some specific SQL dialects."
    to="/core/manage_and_access_data/connect_to_data/sql" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Request data"
    description="Request data from a previously defined Data Source and Data Asset."
    to="/core/manage_and_access_data/request_data" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Manage data objects

Retrieve, view, and delete data objects, or use them to retrieve data for use within GX.

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Manage Data Sources"
    description="Manage the objects that connect GX to your data."
    to="/core/manage_and_access_data/manage_data_sources" 
    icon="/img/expectation_icon.svg" 
  />

  <LinkCard 
    topIcon 
    label="Manage Data Assets"
    description="Manage the objects that tell GX which sets of records are relevant to your use cases."
    to="/core/manage_and_access_data/manage_data_assets" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Manage Batch Requests"
    description="Manage the objects that retrieve data from a Data Asset."
    to="/core/manage_and_access_data/manage_batch_requests" 
    icon="/img/expectation_icon.svg" 
  />

<LinkCard 
    topIcon 
    label="Manage Batches"
    description="Manage the objects that represent your retrieved data."
    to="/core/manage_and_access_data/manage_batches" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>