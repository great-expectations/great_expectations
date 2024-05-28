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
import SqliteDataSource from './_create_a_data_source/_sqlite.md';
import SnowflakeSqlDataSource from './_create_a_data_source/_snowflake.md';
import BigQuerySqlDataSource from './_create_a_data_source/_big_query.md';
import DatabricksSqlDataSource from './_create_a_data_source/_databricks.md';
import OtherSqlDataSource from './_create_a_data_source/_other_sql.md';
import CreateAsset from './_create_a_data_asset/_create_a_data_asset.md';
import CreateBatchDefinition from './_create_a_batch_definition/_create_a_batch_definition.md'

To connect to your SQL data, you first create a Data Source telling GX where your database resides and how to connect to it.  You configure Data Assets for your Data Source to tell GX which sets of records you want to be able to access from your Data Source.  Finally, Batch Definitions allow you to request all the records retrieved from a Data Asset or further partition the returned records based on the contents of a date and time field.

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
<SqliteDataSource/>
</TabItem>

<TabItem value="snowflake" label="Snowflake">
<SnowflakeSqlDataSource/>
</TabItem>

<TabItem value="databricks" label="Databricks SQL">
<DatabricksSqlDataSource/>
</TabItem>

<TabItem value="bigquery" label="BigQuery SQL">
<BigQuerySqlDataSource/>
</TabItem>

<TabItem value="other_sql" label="Other SQL">
<OtherSqlDataSource/>
</TabItem>

</Tabs>

## Create a Data Asset

<CreateAsset/>

## Create a Batch Definition

<CreateBatchDefinition/>