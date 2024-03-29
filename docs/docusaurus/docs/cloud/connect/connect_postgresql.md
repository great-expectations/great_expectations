---
sidebar_label: 'Connect GX Cloud to PostgreSQL'
title: 'Connect GX Cloud to PostgreSQL'
description: Connect GX Cloud to a PostgreSQL Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

To validate data stored in a PostgreSQL database from GX Cloud, you must add the GX Agent to your deployment environment. The GX Agent acts as an intermediary between GX Cloud and PostgreSQL and allows you to securely access and validate your data in GX Cloud.

New to GX Cloud and not sure that it's the right solution for your organization? See [Try GX Cloud](../try_gx_cloud.md).

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud) with [Admin or Editor permissions](../about_gx.md#roles-and-responsibilities).

- You have deployed the GX Agent. See [Deploy the GX Agent](../deploy_gx_agent.md).

- You have a PostgreSQL database, schema, and table.

- To improve data security, GX recommends creating a separate PostgreSQL user for your GX Cloud connection.

- [pgAdmin (optional)](https://www.pgadmin.org/download/)

- You have stopped all local running instances of the GX Agent.

## Prepare your PostgreSQL environment

1. In pgAdmin, select a database.

2. Click **Tools** > **Query Tool**.

3. Copy and paste the following code into the **Query** pane to create and assign the `gx_ro` role and allow GX Cloud to access to all `public` schemas and tables on a specific database:

   ```sql
   CREATE ROLE gx_ro WITH LOGIN PASSWORD '<your_password>';
   GRANT CONNECT ON DATABASE <your_database> TO gx_ro;
   GRANT USAGE ON SCHEMA public TO gx_ro;
   GRANT SELECT ON ALL TABLES in SCHEMA public TO gx_ro;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gx_ro;
   ```

   Replace `<your_password>` and `<your_database>` with your own values. `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gx_ro;` is optional and gives the `gx_ro` user access to all future tables in the defined schema.

4. Click **Execute/Refresh**.

## Next steps

- [Create a Data Asset](../data_assets/manage_data_assets.md#create-a-data-asset)

- [Invite users](../users/manage_users.md#invite-a-user)
