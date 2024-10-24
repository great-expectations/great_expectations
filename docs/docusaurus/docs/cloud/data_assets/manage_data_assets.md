---
sidebar_label: 'Manage Data Assets'
title: 'Manage Data Assets'
description: Create and manage Data Assets in GX Cloud.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

A Data Asset is a collection of records that you create when you connect to your Data Source. When you connect to your Data Source, you define a minimum of one Data Asset. You use these Data Assets to create the Batch Requests that select the data that is provided to your Expectations.

<!-- [//]: # (TODO: To learn more about Data Assets, see Data Asset.) -->


## Create a Data Asset

Create a Data Asset to define the data you want GX Cloud to access. To connect to Data Assets for a Data Source not currently available in GX Cloud, see [Connect to data](/core/connect_to_data/connect_to_data.md) in the GX Core documentation. 

<Tabs
  groupId="manage-data-assets"
  defaultValue='Snowflake'
  values={[
  {label: 'Snowflake', value:'Snowflake'},
  {label: 'PostgreSQL', value:'PostgreSQL'},
  ]}>
<TabItem value="Snowflake">

Define the data you want GX Cloud to access within Snowflake. 

### Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud).

- You have a Snowflake database, schema, and table.

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you have SELECT privileges on the table you are validating. To improve data security, GX recommends using a separate Snowflake user service account to connect to GX Cloud.

- You know your Snowflake password.

### Connect to a Snowflake Data Asset

1. In GX Cloud, click **Data Assets** > **New Data Asset**.

2. Click the **New Data Source** tab and then select **Snowflake**.

3. Select one of the following options:

    - If you're connecting to an org-hosted Snowflake Data Asset for the first time, copy the code and see [Connect GX Cloud to Snowflake](../connect/connect_snowflake.md).

    - If you're testing GX Cloud features and functionality in a self-hosted environment, click **I have created a GX Cloud user with valid permissions** and then click **Continue**.

4. Enter a meaningful name for the Data Asset in the **Data Source name** field.

5. Optional. To use a connection string to connect to a Data Source, click the **Use connection string** selector, enter a connection string, and then move to step 6. The connection string format is: `snowflake://<user_login_name>:<password>@<accountname>`.

6. Complete the following fields:

    - **Account identifier**: Enter your Snowflake organization and account name separated by a hyphen (`oraganizationname-accountname`) or your account name and a legacy account locator separated by a period (`accountname.region`). The legacy account locator value must include the geographical region. For example, `us-east-1`. 
    
        To locate your Snowflake organization name, account name, or legacy account locator values see [Finding the Organization and Account Name for an Account](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account) or [Using an Account Locator as an Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier#using-an-account-locator-as-an-identifier).
    
    - **Username**: Enter the username you use to access Snowflake.

    - **Password**: Enter a Snowflake password. To improve data security, GX recommends using a Snowflake service account to connect to GX Cloud.

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored. In Snowsight, click **Data** > **Databases**. In the Snowflake Classic Console, click **Databases**.
 
    - **Schema**: Enter the name of the Snowflake schema (table) where the data you want to validate is stored.

    - **Warehouse**: Enter the name of your Snowflake database warehouse. In Snowsight, click **Admin** > **Warehouses**. In the Snowflake Classic Console, click **Warehouses**.

    - **Role**: Enter your Snowflake role.

7. Click **Connect**.

8. Select tables to import as Data Assets:

    - Check the box next to a table name to add that table as an asset.
    
    - At least one table must be added.

    - To search for a specific table type the table's name in the **Search** box above the list of tables.
            
    - To add all of the available tables check the box for **All Tables**.



9. Click **Add Asset**.

10. Create an Expectation. See [Create an Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).

</TabItem>
<TabItem value="PostgreSQL">

Define the data you want GX Cloud to access within PostgreSQL.

### Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud).

- You have a PostgreSQL database, schema, and table.

- You have a [PostgreSQL instance](https://www.postgresql.org/download/). To improve data security, GX recommends using a separate user service account to connect to GX Cloud.

- You know your PostgreSQL access credentials.

### Connect to a PostgreSQL Data Asset 

1. In GX Cloud, click **Data Assets** > **New Data Asset**.

2. Click the **New Data Source** tab and then select **PostgreSQL**.

3. Select one of the following options:

    - If you're connecting to an org-hosted PostgreSQL Data Asset for the first time, copy the code and see [Connect GX Cloud to PostgreSQL](../connect/connect_postgresql.md).

    - If you're testing GX Cloud features and functionality in a self-hosted environment, click **I have created a GX Cloud user with valid permissions** and then click **Continue**.

4. Enter a meaningful name for the Data Asset in the **Data Source name** field.

5. Enter a connection string in the **Connection string** field. The connection string format is `postgresql+psycopg2//YourUserName:YourPassword@YourHostname:5432/YourDatabaseName`. 

6. Click **Connect**.

7. Select tables to import as Data Assets:

    - Check the box next to a table name to add that table as an asset.

    - At least one table must be added.

    - To search for a specific table type the table's name in the **Search** box above the list of tables.

    - To add all of the available tables check the box for **All Tables**.


8. Click **Add Asset**.

9. Create an Expectation. See [Create an Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).

</TabItem>
</Tabs>


## View Data Asset metrics

Data Asset metrics provide you with insight into the data you can use for your data validations. 

1. In GX Cloud, click **Data Assets** and then select a Data Asset in the **Data Assets** list.

2. Click the **Overview** tab.

    When you select a new Data Asset, schema data is automatically fetched.

3. Optional. Select one of the following options:

    - Click **Profile Data** if you have not previously returned all available metrics for a Data Asset.

    - Click **Refresh** to refresh the Data Asset metrics.

### Available Data Asset metrics

The following table lists the available Data Asset metrics.

| Column                                   | Description                                               | 
|------------------------------------------|-----------------------------------------------------------|
| **Row Count**                            | The number of rows within a Data Asset.                   | 
| **Column**                               | A column within your Data Asset.                          | 
| **Type**                                 | The data storage type in the Data Asset column.           | 
| **Min**                                  | For numeric columns the lowest value in the column.       | 
| **Max**                                  | For numeric columns, the highest value in the column.     | 
| **Mean**                                 | For numeric columns, the average value with the column.<br/> This is determined by dividing the sum of all values in the Data Asset by the number of values.  |
| **Median**                                 | For numeric columns, the value in the middle of a data set.<br/> 50% of the data within the Data Asset has a value smaller or equal to the median, and 50% of the data within the Data Asset has a value that is higher or equal to the median.  |
| **Null %**                                | The percentage of missing values in a column.             |

## Add an Expectation to a Data Asset column

When you create an Expectation after fetching metrics for a Data Asset, the column names and some values are autopopulated for you and this can simplify the creation of new Expectations. Data Asset Metrics can also help you determine what Expectations might be useful and how they should be configured. When you create new Expectations after fetching Data Asset Metrics, you can add them to an existing Expectation Suite, or you can create a new Expectation Suite and add the Expectations to it. 

1. In GX Cloud, click **Data Assets** and then select a Data Asset in the **Data Assets** list.

2. Click the **Overview** tab.

    When you select a new Data Asset, schema data is automatically fetched.

3. Optional. Select one of the following options:

    - Click **Profile Data** if you have not previously returned all available metrics for a Data Asset.

    - Click **Refresh** to refresh the Data Asset metrics.

4. Click **New Expectation**.

5. Select one of the following options:

    - To add an Expectation to a new Expectation Suite, click the **New Suite** tab and then enter a name for the new Expectation Suite.

    - To add an Expectation to an existing Expectation Suite, click the **Existing Suite** tab and then select an existing Expectation Suite.

6. Select an Expectation type. See [Available Expectation types](/cloud/expectations/manage_expectations.md#available-expectation-types).

7. Complete the fields in the **Create Expectation** pane.

8. Click **Save** to add the Expectation, or click **Save & Add More** to add additional Expectations.


## Add a Data Asset to an Existing Data Source

Additional Data Assets can only be added to Data Sources created in GX Cloud.

1. In GX Cloud, click **Data Assets** and then select **New Data Asset**.

2. Click the **Existing Data Source** tab and then select a Data Source.

3. Click **Add Data Asset**.

4. Select tables to import as Data Assets:

    - Check the box next to a table name to add that table as an asset.

    - At least one table must be added.

    - To search for a specific table type the table's name in the **Search** box above the list of tables.

    - To add all of the available tables check the box for **All Tables**.


5. Click **Add Asset**.


## Edit Data Source settings

Edit Data Source settings to update Data Source connection information or access credentials. You can only edit the settings of Data Sources created in GX Cloud.

<Tabs
  groupId="manage-data-assets"
  defaultValue='Snowflake'
  values={[
  {label: 'Snowflake', value:'Snowflake'},
  {label: 'PostgreSQL', value:'PostgreSQL'},
  ]}>
<TabItem value="Snowflake">

1. In GX Cloud, click **Data Assets**.

2. Click **Manage Data Sources**.

3. Click **Edit Data Source** for the Snowflake Data Source you want to edit.

4. Optional. Edit the Data Source name.

5. Optional. If you used a connection string to connect to the Data Source, click the **Connection string** tab and edit the Data Source connection string.

6. Optional. If you're not using a connection string, edit the following fields:
    
     - **Account identifier**: Enter new Snowflake account or locator information. The locator value must include the geographical region. For example, `us-east-1`. To locate these values see [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier).
     
     - **Username**: Enter a new Snowflake username.

    - **Password**: Enter the password for the Snowflake user you're connecting to GX Cloud. To improve data security, GX recommends using a Snowflake service account to connect to GX Cloud.

    - **Database**: Enter a new Snowflake database name.
 
    - **Schema**: Enter a new schema name.

    - **Warehouse**: Enter a new Snowflake database warehouse name.

    - **Role**: Enter a new Snowflake role.

7. Click **Save**.

</TabItem>
<TabItem value="PostgreSQL">

1. In GX Cloud, click **Data Assets**.

2. Click **Manage Data Sources**.

3. Click **Edit Data Source** for the PostgreSQL Data Source you want to edit.

4. Optional. Edit the Data Source name.

5. Optional. Click **Show** in the **Connection string** field and then edit the Data Source connection string.

6. Click **Save**.


</TabItem>
</Tabs>

## Edit a Data Asset

You can only edit the settings of Data Assets created in GX Cloud.

1. In GX Cloud, click **Data Assets** and in the Data Assets list click **Edit Data Asset** for the Data Asset you want to edit.

2. Edit the following fields:

    - **Table name**: Enter a new name for the Data Asset table.

    - **Data Asset name**: Enter a new name for the Data Asset. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

3. Click **Save**.

## Data Source credential management

To connect to your Data Source in GX Cloud, there are two methods for managing credentials:

-  **Direct input**: You can input credentials directly into GX Cloud. These credentials are stored in GX Cloud and securely encrypted at rest and in transit. When Data Source credentials have been directly provided, they can be used to connect to a Data Source in any GX Cloud deployment pattern.

- **Environment variable substitution**: To enhance security, you can use environment variables to manage sensitive connection parameters or strings. For example, instead of directly including your database password in configuration settings, you can use a variable reference like `${MY_DATABASE_PASSWORD}`. When using environment variable substitution, your password is not stored or transmitted to GX Cloud.

   :::warning[Environment variable substitution support]
   Environment variable substitution is not supported in fully-hosted deployments.
   :::

   - **Configure the environment variable**: Enter the name of your environment variable, enclosed in `${}`, into the Data Source setup form. For instance, you might use `${MY_DATABASE_PASSWORD}`.

   - **Inject the variable into your GX Agent container or environment**: When running the GX Agent Docker container, include the environment variable in the command. For example:
   
      ```bash title="Terminal input"
      docker run -it -e MY_DATABASE_PASSWORD=<YOUR_DATABASE_PASSWORD> -e GX_CLOUD_ACCESS_TOKEN=<YOUR_ACCESS_TOKEN> -e GX_CLOUD_ORGANIZATION_ID=<YOUR_ORGANIZATION_ID> greatexpectations/agent:stable
      ```

   When running the GX Agent in other Docker-based service, including Kubernetes, ECS, ACI, and GCE, use the service's instructions to set and provide environment variables to the running container.

   When using environment variable substitution in a read-only deployment, set the environment variable in the environment where the GX Core Python client is running.

## Delete a Data Asset

1. In GX Cloud, click **Settings** > **Datasources**.

2. Click **Delete** for the Data Source and the associated Data Assets you want to delete.

3. Click **Delete**.

