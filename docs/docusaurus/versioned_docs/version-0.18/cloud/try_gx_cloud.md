---
sidebar_label: 'Try GX Cloud'
title: 'Try GX Cloud'
id: try_gx_cloud
description: Try GX Cloud features and functionality.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

If you're new to GX Cloud, start here to learn how you can quickly connect to your Data Assets and validate data.

If you've tested GX Cloud features and functionality and discovered it's a great solution for your organization, see [Connect GX Cloud](./connect/connect_lp.md).

## Prerequisites

- You have deployed the GX Agent.

- You have a [Docker instance](https://docs.docker.com/get-docker/).

- You've reviewed the prerequisites for the Data Asset you'll create. See [Create a Data Asset](#create-a-data-asset).

## Create a Data Asset

Create a Data Asset to define the data you want GX Cloud to access.

<Tabs
  groupId="try-gx-cloud"
  defaultValue='Snowflake'
  values={[
  {label: 'Snowflake', value:'Snowflake'},
  {label: 'PostgreSQL', value:'PostgreSQL'},
  ]}>
<TabItem value="Snowflake">

Define the data you want GX Cloud to access within Snowflake.

### Prerequisites

- You have a Snowflake database, schema, and table.

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you have SELECT privileges on the table you are validating. To improve data security, GX recommends using a separate Snowflake user service account to connect to GX Cloud.

- You know your Snowflake password.

### Create a Snowflake Data Asset

Create a Data Asset to define the data you want GX Cloud to access within Snowflake. 

1. In GX Cloud, click **Data Assets** > **New Data Asset**.

2. Click the **New Data Source** tab and then select **Snowflake**.

3. Enter a meaningful name for the Data Asset in the **Data Source name** field.

4. Optional. To use a connection string to connect to a Data Source, click the **Use connection string** selector, enter a connection string, and then move to step 6. The connection string format is: `snowflake://<user_login_name>:<password>@<accountname>`.

5. Complete the following fields:

    - **Username**: Enter the username you use to access Snowflake.

    - **Account identifier**: Enter your Snowflake organization and account name separated by a hyphen (`oraganizationname-accountname`) or your account name and a legacy account locator separated by a period (`accountname.region`). The legacy account locator value must include the geographical region. For example, `us-east-1`. 
    
        To locate your Snowflake organization name, account name, or legacy account locator values see [Finding the Organization and Account Name for an Account](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account) or [Using an Account Locator as an Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier#using-an-account-locator-as-an-identifier).

    - **Password**: Enter a Snowflake password. To improve data security, GX recommends using a Snowflake service account to connect to GX Cloud.

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored. In Snowsight, click **Data** > **Databases**. In the Snowflake Classic Console, click **Databases**.
 
    - **Schema**: Enter the name of the Snowflake schema (table) where the data you want to validate is stored.

    - **Warehouse**: Enter the name of your Snowflake database warehouse. In Snowsight, click **Admin** > **Warehouses**. In the Snowflake Classic Console, click **Warehouses**.

    - **Role**: Enter your Snowflake role.

6. Optional. Select **Test connection** to test the Data Source connection. Testing the connection to the Data Source is a preventative measure that makes sure the connection configuration is correct. This verification can help you avoid errors and can reduce troubleshooting downtime.

7. Click **Continue**.

8. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.
    
    - **Data Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

    - **Query**: When **Query Asset** is selected, enter the query that you want to run on the table. 

9. Select the **Complete Asset** tab to provide all Data Asset records to your Expectations and validations, or select the **Batches** tab to use subsets of Data Asset records for your Expectations and validations. If you selected the **Batches** tab, complete the following fields:

    - **Split Data Asset by** - Select **Year** to partition Data Asset records by year, select **Year - Month** to partition Data Asset records by year and month, or select **Year - Month - Day** to partition Data Asset records by year, month, and day.

    - **Column of datetime type** - Enter the name of the column containing the date and time data.

10. Optional. Select **Add Data Asset** to add additional tables or queries and repeat steps 8 and 9.

11. Click **Finish**. The Data Asset(s), a default empty Expectation Suite, and a default Checkpoint are created.

</TabItem>
<TabItem value="PostgreSQL">

Define the data you want GX Cloud to access within PostgreSQL.

### Prerequisites

- You have a PostgreSQL database, schema, and table.

- You have a [PostgreSQL instance](https://www.postgresql.org/download/). To improve data security, GX recommends using a separate user service account to connect to GX Cloud.

### Create a PostgreSQL Data Asset 

1. In GX Cloud, click **Data Assets** > **New Data Asset**.

2. Click the **New Data Source** tab and then select **PostgreSQL**.

3. Enter a meaningful name for the Data Asset in the **Data Source name** field.

4. Enter a connection string in the **Connection string** field. The connection string format is `postgresql+psycopg2//YourUserName:YourPassword@YourHostname:5432/YourDatabaseName`. 

5. Optional. Select **Test connection** to test the Data Source connection. Testing the connection to the Data Source is a preventative measure that makes sure the connection configuration is correct. This verification can help you avoid errors and can reduce troubleshooting downtime.

6. Click **Continue**.

7. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.
    
    - **Data Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

    - **Query**: When **Query Asset** is selected, enter the query that you want to run on the table. 

8. Select the **Complete Asset** tab to provide all Data Asset records to your Expectations and validations, or select the **Batches** tab to use subsets of Data Asset records for your Expectations and validations. If you selected the **Batches** tab, complete the following fields:

    - **Split Data Asset by** - Select **Year** to partition Data Asset records by year, select **Year - Month** to partition Data Asset records by year and month, or select **Year - Month - Day** to partition Data Asset records by year, month, and day.

    - **Column of datetime type** - Enter the name of the column containing the date and time data.

9. Optional. Select **Add Data Asset** to add additional tables or queries and repeat steps 8 and 9.

10. Click **Finish**. The Data Asset(s), a default empty Expectation Suite, and a default Checkpoint are created. 

</TabItem>
</Tabs>


## Add an Expectation

An Expectation is a verifiable assertion about your data. They make implicit assumptions about your data explicit.

1. In the **Data Assets** list, click the Data Asset name.

2. Click the **Expectations** tab.

3. Click **New Expectation**.

4. Select an Expectation type, enter the column name, and then complete the optional fields. To view descriptions of the available Expectation types, see [Available Expectations](./expectations/manage_expectations.md#available-expectations).

    If you prefer to work in a code editor, click the **JSON Editor** tab and define your Expectation parameters in the code pane.

5. Click **Save**. The Expectation is added to the default Expectation Suite.

6. Optional. Repeat steps 3 to 5 to add additional Expectations.

## Validate Expectations

1. Click **Validate**.

2. When the confirmation message appears, click **See results**, or click the **Validations** tab.

## Next steps

- [Connect GX Cloud](/cloud/connect/connect_lp.md)
