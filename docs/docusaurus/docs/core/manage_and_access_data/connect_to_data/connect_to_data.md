---
title: Connect to data
hide_feedback_survey: true
hide_title: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

import InProgress from '../../_core_components/_in_progress.md';


<OverviewCard title={frontMatter.title}>
  Great Expectations (GX) differentiates between the locations in which data is stored and the sets of data that are available at those locations.  In GX, Data Sources manage the process of accessing locations in which data is stored.  Data Assets define sets of data that can be accessed from those Data Sources.  To connect to your data you will first define a Data Source to tell GX how to access the data in question and then define one or more Data Assets to tell GX which sets of data to make available.
</OverviewCard>

## File system Data Sources

File system Data Sources connect GX to data that is stored as one or more files (such as `.csv` or `.parquet` files) within a directory hierarchy.

<LinkCardGrid>

<LinkCard 
    topIcon 
    label="Basic file system"
    description="Manage data stored as files on a local or networked file system."
    to="/core/manage_and_access_data/connect_to_data/file_system?data-source=filesystem" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Amazon S3"
    description="Manage data stored as files in an Amazon S3 bucket."
    to="/core/manage_and_access_data/connect_to_data/file_system?data-source=amazon" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Azure Blob Storage"
    description="Manage data stored as files in Azure Blob Storage."
    to="/core/manage_and_access_data/connect_to_data/file_system?data-source=azure" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Google Cloud Storage"
    description="Manage data stored as files in Google Cloud Storage."
    to="/core/manage_and_access_data/connect_to_data/file_system?data-source=gcs" 
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
    to="/core/manage_and_access_data/connect_to_data/in_memory?connector=pandas" 
    icon="/img/expectation_icon.svg" 
  />
<LinkCard 
    topIcon 
    label="Spark"
    description="Manage Data Sources for data that has been loaded into memory with Spark."
    to="/core/manage_and_access_data/connect_to_data/in_memory?connector=spark" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## SQL Data Sources

SQL Data Sources connect GX to data stored in SQL databases, with support for some specific SQL dialects.

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Snowflake"
    description="Manage Data Sources that connect to Snowflake SQL databases."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=snowflake" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="PostgreSQL"
    description="Manage Data Sources that connect to PostgreSQL databases."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=postgresql" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="SQLite"
    description="Manage Data Sources that connect to SQLite databases."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=sqlite" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Databricks"
    description="Manage Data Sources that connect to Databricks SQL databases."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=databricks" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="BigQuery"
    description="Manage Data Sources that connect to BigQuery SQL databases."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=bigquery" 
    icon="/img/expectation_icon.svg" 
  />
  <LinkCard 
    topIcon 
    label="Generic SQL"
    description="Manage Data Sources that connect to SQL databases without utilizing a specific SQL dialect."
    to="/core/manage_and_access_data/connect_to_data/sql?data-source=unspecified" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>

## Request data

<LinkCardGrid>

  <LinkCard 
    topIcon 
    label="Request data"
    description="Define a Batch Request and retrieve data from a Data Asset."
    to="/core/manage_and_access_data/connect_to_data/request_data.md" 
    icon="/img/expectation_icon.svg" 
  />

</LinkCardGrid>