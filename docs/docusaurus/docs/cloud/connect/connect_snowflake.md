---
sidebar_label: 'Connect GX Cloud to Snowflake'
title: 'Connect GX Cloud to Snowflake'
description: Connect GX Cloud to a Snowflake Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

New to GX Cloud and not sure that it's the right solution for your organization? See [Try GX Cloud](../try_gx_cloud.md).

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud) with [Admin or Editor permissions](../about_gx.md#roles-and-responsibilities).

- You have deployed the GX Agent. See [Deploy the GX Agent](../deploy_gx_agent.md).

- You have a Snowflake database, schema, and table.

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you have SELECT privileges on the table you are validating. To improve data security, GX recommends using a separate Snowflake user service account to connect to GX Cloud.

- You know your Snowflake password.

- You have stopped all local running instances of the GX Agent.

## Prepare your Snowflake environment

You can use an existing Snowflake warehouse, but GX recommends creating a separate warehouse for GX Cloud to simplify cost management and optimize performance.

1. In Snowflake Snowsight, click **Worksheets** > **Add** > **SQL Worksheet**.

2. Copy and paste the following code into the SQL worksheet:

   ```sh title="SQL worksheet"
   use role accountadmin;
   create user gx_user password="secure_password";
   create role gx_role;
   grant role gx_role to user gx_user;
   ```
3. Replace `secure_password` with your value.

4. Select **Run All** to define your user password, create a new GX role (`gx_role`), and assign the password and role to a new user (`gx_user`).

    ![Snowflake Run All](/img/run_all.png)

5. Copy the following code and paste it into the SQL worksheet:

   ```sh title="SQL worksheet"
   create warehouse gx_wh
   warehouse_size=xsmall 
   auto_suspend=10  
   auto_resume=true
   initially_suspended=true;
   ```
    The settings in the code example optimize cost and performance. Adjust them to suit your business requirements.

6. Select **Run All** to create a new warehouse (`gx_wh`) for the GX Agent.

7. Copy the following code and paste it into the SQL worksheet:

   ```sh title="SQL worksheet"
   grant usage, operate on warehouse gx_wh to role gx_role;
   grant usage on database "database_name" to role gx_role;
   grant usage on schema "database_name.schema_name" to role gx_role;
   grant select on all tables in schema "database_name.schema_name" to role gx_role;
   grant select on future tables in schema "database_name.schema_name" to role gx_role; 
   ```
   `grant select on future tables in schema "database_name.schema_name" to role gx_role;` is optional and gives the user with the `gx_role` role access to all future tables in the defined schema.

8. Replace `database_name` and `schema_name` with the names of the database and schema you want to access in GX Cloud.

9. Select **Run All** to allow the user with the `gx_role` role to access data on the Snowflake database and schema.

## Get your GX Cloud access token and organization ID

You'll need your access token and organization ID to set your access credentials. Don't commit your access credentials to your version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your access token and then save the file. 

   GX recommends deleting the temporary file after you set the environment variables.

## Deploy the GX Agent

See

## Next steps

- [Create a Data Asset](../data_assets/manage_data_assets.md#create-a-data-asset)

- [Invite users](../users/manage_users.md#invite-a-user)

