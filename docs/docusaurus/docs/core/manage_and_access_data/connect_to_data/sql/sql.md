---
title: "Connect to SQL database data"
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import BigqueryPrerequisites from './_bigquery/_prerequisites.md'
import BigqueryDataSource from './_bigquery/_add_data_source.md'
import BigqueryDataAsset from './_bigquery/_add_data_asset.md'
import BigqueryNextSteps from './_bigquery/_next_steps.md'

import DatabricksPrerequisites from './_databricks/_prerequisites.md'
import DatabricksDataSource from './_databricks/_add_data_source.md'
import DatabricksDataAsset from './_databricks/_add_data_asset.md'
import DatabricksNextSteps from './_databricks/_next_steps.md'

import PostgresqlPrerequisites from './_postgresql/_prerequisites.md'
import PostgresqlDataSource from './_postgresql/_add_data_source.md'
import PostgresqlDataAsset from './_postgresql/_add_data_asset.md'
import PostgresqlNextSteps from './_postgresql/_next_steps.md'

import SnowflakePrerequisites from './_snowflake/_prerequisites.md'
import SnowflakeDataSource from './_snowflake/_add_data_source.md'
import SnowflakeDataAsset from './_snowflake/_add_data_asset.md'
import SnowflakeNextSteps from './_snowflake/_next_steps.md'

import SqlitePrerequisites from './_sqlite/_prerequisites.md'
import SqliteDataSource from './_sqlite/_add_data_source.md'
import SqliteDataAsset from './_sqlite/_add_data_asset.md'
import SqliteNextSteps from './_sqlite/_next_steps.md'

import UnspecifiedPrerequisites from './_unspecified/_prerequisites.md'
import UnspecifiedDataSource from './_unspecified/_add_data_source.md'
import UnspecifiedDataAsset from './_unspecified/_add_data_asset.md'
import UnspecifiedNextSteps from './_unspecified/_next_steps.md'

Use the information provided here to connect to Data Assets that are stored in a SQL database using a specified SQL dialect.

Choose the SQL dialect for your database:

<Tabs
  queryString="data-source"
  groupId="sql-data-source"
  defaultValue='snowflake'
  values={[
  {label: 'Snowflake', value:'snowflake'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Databricks SQL', value:'databricks'},
  {label: 'BigQuerySQL', value:'bigquery'},
  {label: 'Unspecified dialect', value:'unspecified'},
  ]}>

  <TabItem value="snowflake">
  </TabItem>

  <TabItem value="postgresql">
  </TabItem>

  <TabItem value="sqlite">
  </TabItem>

  <TabItem value="databricks">
  </TabItem>

  <TabItem value="bigquery">
  </TabItem>

  <TabItem value="unspecified">
  </TabItem>

</Tabs>

## Prerequisites

<Tabs
  queryString="data-source"
  groupId="sql-data-source"
  defaultValue='snowflake'
  values={[
  {label: 'Snowflake', value:'snowflake'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Databricks SQL', value:'databricks'},
  {label: 'BigQuerySQL', value:'bigquery'},
  {label: 'Unspecified dialect', value:'unspecified'},
  ]}>

  <TabItem value="snowflake">
<SnowflakePrerequisites/>
  </TabItem>

  <TabItem value="postgresql">
<PostgresqlPrerequisites/>
  </TabItem>

  <TabItem value="sqlite">
<SqlitePrerequisites/>
  </TabItem>

  <TabItem value="databricks">
<DatabricksPrerequisites/>
  </TabItem>

  <TabItem value="bigquery">
<BigqueryPrerequisites/>
  </TabItem>

  <TabItem value="unspecified">
<UnspecifiedPrerequisites/>
  </TabItem>

</Tabs>

## Create a Data Source

<Tabs
  queryString="data-source"
  groupId="sql-data-source"
  defaultValue='snowflake'
  values={[
  {label: 'Snowflake', value:'snowflake'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Databricks SQL', value:'databricks'},
  {label: 'BigQuerySQL', value:'bigquery'},
  {label: 'Unspecified dialect', value:'unspecified'},
  ]}>

  <TabItem value="snowflake">
<SnowflakeDataSource/>
  </TabItem>

  <TabItem value="postgresql">
<PostgresqlDataSource/>
  </TabItem>

  <TabItem value="sqlite">
<SqliteDataSource/>
  </TabItem>

  <TabItem value="databricks">
<DatabricksDataSource/>
  </TabItem>

  <TabItem value="bigquery">
<BigqueryDataSource/>
  </TabItem>

  <TabItem value="unspecified">
<UnspecifiedDataSource/>
  </TabItem>

</Tabs>

## Add a Data Asset to a Data Source

<Tabs
  queryString="data-source"
  groupId="sql-data-source"
  defaultValue='snowflake'
  values={[
  {label: 'Snowflake', value:'snowflake'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Databricks SQL', value:'databricks'},
  {label: 'BigQuerySQL', value:'bigquery'},
  {label: 'Unspecified dialect', value:'unspecified'},
  ]}>

  <TabItem value="snowflake">
<SnowflakeDataAsset/>
  </TabItem>

  <TabItem value="postgresql">
<PostgresqlDataAsset/>
  </TabItem>

  <TabItem value="sqlite">
<SqliteDataAsset/>
  </TabItem>

  <TabItem value="databricks">
<DatabricksDataAsset/>
  </TabItem>

  <TabItem value="bigquery">
<BigqueryDataAsset/>
  </TabItem>

  <TabItem value="unspecified">
<UnspecifiedDataAsset/>
  </TabItem>

</Tabs>

## Next steps

<Tabs
  queryString="data-source"
  groupId="sql-data-source"
  defaultValue='snowflake'
  values={[
  {label: 'Snowflake', value:'snowflake'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Databricks SQL', value:'databricks'},
  {label: 'BigQuerySQL', value:'bigquery'},
  {label: 'Unspecified dialect', value:'unspecified'},
  ]}>

  <TabItem value="snowflake">
<SnowflakeNextSteps/>
  </TabItem>

  <TabItem value="postgresql">
<PostgresqlNextSteps/>
  </TabItem>

  <TabItem value="sqlite">
<SqliteNextSteps/>
  </TabItem>

  <TabItem value="databricks">
<DatabricksNextSteps/>
  </TabItem>

  <TabItem value="bigquery">
<BigqueryNextSteps/>
  </TabItem>

  <TabItem value="unspecified">
<UnspecifiedNextSteps/>
  </TabItem>

</Tabs>