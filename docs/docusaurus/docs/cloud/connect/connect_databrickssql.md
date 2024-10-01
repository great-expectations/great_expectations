---
sidebar_label: 'Connect GX Cloud to Databricks SQL'
title: 'Connect GX Cloud to Databricks SQL'
description: Connect GX Cloud to a Databricks SQL Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud) with [Admin or Editor permissions](/cloud/users/manage_users.md#roles-and-responsibilities).

- You have a Databricks SQL catalog, schema, and table.

- To improve data security, GX recommends creating a separate Databricks SQL [service principal](https://docs.databricks.com/en/admin/users-groups/service-principals.html#manage-service-principals-in-your-account) for your GX Cloud connection.


## Connect to a Databricks SQL Data Asset

1. In GX Cloud, click **Data Assets** > **New Data Asset** > **Databricks SQL**.

2. Enter a meaningful name for the Data Source in the **Data Source name** field.

3. Enter a connection string in the **Connection string** field. The connection string format is `databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}`.
    - Check the instructions to create a GX-specific user in your Databricks SQL catalog by clicking "See instructions"

4. Click **Connect**.

5. Select tables to import as Data Assets:

    - Check the box next to a table name to add that table as an asset.

    - At least one table must be added.

    - To search for a specific table type the table's name in the Search box above the list of tables.

    - To add all of the available tables check the box for All Tables.

6. Click **Add Asset**.

7. Create an Expectation. See [Create an Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).
