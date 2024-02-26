---
title: Manage Data Sources
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';


import InProgress from '../../_core_components/_in_progress.md';

Data Sources manage the process of accessing locations in which data is stored.  GX utilizes Data Sources to connect to data storage and provide a unified API for defining sets of data that can be retrieved.

## File system Data Sources

File system Data Sources connect GX to data that is stored as one or more files (such as `.csv` or `.parquet` files) within a directory hierarchy.

<LinkCardGrid>

<LinkCard 
    topIcon 
    label="Basic file system"
    description="Manage data stored as files on a local or networked file system."
    to="/core/connect_to_data/manage_data_sources/file_system" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Amazon S3"
    description="Manage data stored as files in an Amazon S3 bucket."
    to="/" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Azure Blob Storage"
    description="Manage data stored as files in Azure Blob Storage."
    to="/core/connect_to_data/manage_data_sources/azure_blob_storage" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Google Cloud Storage"
    description="Manage data stored as files in Google Cloud Storage."
    to="/core/connect_to_data/manage_data_sources/google_cloud_storage" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## In memory Data Sources

In memory Data Sources connect GX to data that has been read into memory.

<LinkCardGrid>

<LinkCard 
    topIcon 
    label="pandas"
    description="Manage Data Sources for data that has been loaded into memory with pandas."
    to="/core/connect_to_data/manage_data_sources/in_memory?connector=pandas" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Spark"
    description="Manage Data Sources for data that has been loaded into memory with Spark."
    to="/core/connect_to_data/manage_data_sources/in_memory?connector=spark" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## SQL Data Sources

SQL Data Sources connect GX to data stored in SQL databases, with support for some specific SQL dialects.

<InProgress/>

<LinkCardGrid>

[//]: # (TODO: Update these link cards to point to specific tabs on the sql page)

<LinkCard 
    topIcon 
    label="Generic SQL"
    description="Manage Data Sources that connect to SQL databases without utilizing a specific SQL dialect."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="BigQuery"
    description="Manage Data Sources that connect to BigQuery SQL databases."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Databricks"
    description="Manage Data Sources that connect to Databricks SQL databases."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="PostgreSQL"
    description="Manage Data Sources that connect to PostgreSQL databases."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Snowflake"
    description="Manage Data Sources that connect to Snowflake SQL databases."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="SQLite"
    description="Manage Data Sources that connect to SQLite databases."
    to="/core/connect_to_data/manage_data_sources/sql" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Next steps

- [Manage Data Assets](/core/connect_to_data/manage_data_assets.md)