---
sidebar_label: 'Connect GX Cloud to Snowflake'
title: 'Connect GX Cloud to Snowflake'
description: Connect GX Cloud to a Snowflake Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud) with [Admin or Editor permissions](/cloud/users/manage_users.md#roles-and-responsibilities).

- You have a Snowflake database, schema, and table.

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you have SELECT privileges on the table you are validating. To improve data security, GX recommends using a separate Snowflake user service account to connect to GX Cloud.

- You know your Snowflake password.

## Connect to a Snowflake Data Asset

You can use an existing Snowflake warehouse, but GX recommends creating a separate warehouse for GX Cloud to simplify cost management and optimize performance.

1. In GX Cloud, click **Data Assets** > **New Data Asset** > **Snowflake**.

2. Copy the code in the code pane.

3. Prepare your Snowflake environment:

   - In Snowflake Snowsight, click **Worksheets** > **Add** > **SQL Worksheet**.

   - Paste the code you copied in step 2 into the SQL worksheet.

      Replace `your_password` with your value and `your_database_name` and `your_schema` with the names of the database and schema you want to access in GX Cloud.

      `grant select on future tables in schema "your_database.your_schema" to role gx_role;` is optional and gives the user with the `gx_role` role access to all future tables in the defined schema.

      The settings in the code example optimize cost and performance. Adjust them to suit your business requirements.

   - Select **Run All** to define your user password, create a new GX role (`gx_role`), assign the password and role to a new user (`gx_user`), create a new warehouse (`gx_wh`) for the GX Agent, and allow the user with the `gx_role` role to access data on the Snowflake database and schema.

       ![Snowflake Run All](/img/run_all.png)

4. In GX Cloud, click **I have created a GX Cloud user with valid permissions** and then click **Continue**.

5. Enter a meaningful name for the Data Source in the **Data Source name** field.

6. Optional. To use a connection string to connect to a Data Source, click the **Use connection string** selector, enter a connection string, and then move to step 6. The connection string format is: `snowflake://<user_login_name>:<password>@<accountname>`.

7. Complete the following fields:

     - **Account identifier**: Enter your Snowflake organization and account name separated by a hyphen (`oraganizationname-accountname`) or your account name and a legacy account locator separated by a period (`accountname.region`). The legacy account locator value must include the geographical region. For example, `us-east-1`. 
    
        To locate your Snowflake organization name, account name, or legacy account locator values see [Finding the Organization and Account Name for an Account](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account) or [Using an Account Locator as an Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier#using-an-account-locator-as-an-identifier).
    
    - **Username**: Enter the username you use to access Snowflake.

    - **Password**: Enter a Snowflake password. To improve data security, GX recommends using a Snowflake service account to connect to GX Cloud.

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored. In Snowsight, click **Data** > **Databases**. In the Snowflake Classic Console, click **Databases**.
 
    - **Schema**: Enter the name of the Snowflake schema (table) where the data you want to validate is stored.

    - **Warehouse**: Enter the name of your Snowflake database warehouse. In Snowsight, click **Admin** > **Warehouses**. In the Snowflake Classic Console, click **Warehouses**.

    - **Role**: Enter your Snowflake role.

8. Click **Connect**.

9. Complete the following fields:

    - **Table name**: Enter the name of the Data Source table you're connecting to.
    
    - **Data Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

10. Select the **Complete Asset** tab to provide all Data Asset records to your Expectations and validations, or select the **Batches** tab to use subsets of Data Asset records for your Expectations and validations. If you selected the **Batches** tab, complete the following fields:

    - **Split Data Asset by** - Select **Year** to partition Data Asset records by year, select **Year - Month** to partition Data Asset records by year and month, or select **Year - Month - Day** to partition Data Asset records by year, month, and day.

    - **Column of datetime type** - Enter the name of the column containing the date and time data.

11. Optional. Select **Add Data Asset** to add additional tables or queries and repeat steps 8 and 9.

12. Click **Finish**.

13. Create an Expectation. See [Create an Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).
