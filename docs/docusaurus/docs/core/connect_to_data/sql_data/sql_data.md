---
title: Connect to data using SQL
description: Connect to data in SQL databases and organize that data into Batches for retrieval and validation.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import ConfigureCredentials from '../configure_credentials/_configure_credentials.md';
import PostgreSqlDataSource from './_create_a_data_source/_postgres.md';
import CreateAsset from './_create_a_data_asset/_create_a_data_asset.md';
import CreateBatchDefinition from './_create_a_batch_definition/_create_a_batch_definition.md'

To connect to your SQL data, you first create a Data Source telling GX where your database resides and how to connect to it.  You configure Data Assets for your Data Source to tell GX which sets of records you want to be able to access.  Finally, Batch Definitions allow you to further partition the records in your Data Assets.

GX supports the following SQL dialects:

- PostgreSQL
- SQLite
- Snowflake
- Databricks SQL
- BigQuery SQL

All other SQL dialects are handled through the python module `SQLAlchemy`.  You can find more information on the dialects supported by `SQLAlchemy` on their [dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html) page.

## Configure credentials

<ConfigureCredentials/>

## Create a SQL Data Source

Data Sources tell GX where your data is located and how to connect to it.  With SQL databases this is done through a connection string you will provide.

<Tabs>

<TabItem value="postgresql" label="PostgreSql">
<PostgreSqlDataSource/>
</TabItem>

<TabItem value="sqlite" label="SQLite">
<PostgreSqlDataSource/>
</TabItem>

</Tabs>

## Create a Data Asset

<CreateAsset/>

## Create a Batch Definition

<CreateBatchDefinition/>