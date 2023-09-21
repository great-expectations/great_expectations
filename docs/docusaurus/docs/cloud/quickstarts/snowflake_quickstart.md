---
sidebar_label: 'Snowflake quickstart'
title: 'Snowflake quickstart'
id: snowflake_quickstart
description: Connect Great Expectations to Snowflake Data Assets.
---

In this quickstart, you'll learn how to connect to Snowflake Data Assets.

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have a [Snowflake account](https://greatexpectations.io/cloud) with `ACCOUNTADMIN` access.

## Prepare your environment

1. Download and install the latest Python version. See [Active Python Releases](https://www.python.org/downloads/).

2. Download and install pip. See the [pip documentation](https://pip.pypa.io/en/stable/cli/pip/).

3. Run the following command in an empty base directory inside a Python virtual environment to install GX Cloud and its dependencies:

    ```bash title="Terminal input"
    pip install great_expectations[cloud]
    ```

    It can take several minutes for the installation to complete.

    If you've previously installed GX Cloud, run the following command to upgrade to the latest version:

    ```bash title="Terminal input"
    pip install great_expectations --upgrade
    ```

## Generate your user access token and copy your organization ID

You'll need your user access token and organization ID to set your environment variables. Access tokens shouldn't be committed to version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **Access tokens** pane, click **Create user access token**.

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Admin**. For more information about the available roles, click **Information** (?) .

4. Click **Create**.

5. Copy the access token and store it in a secure location. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field and save it in the same location with your user access token.

## Set the environment variables and run the GX Cloud Agent

1. Set the following environment variables:

    - `GX_CLOUD_ORGANIZATION_ID` - The Organization ID you copied previously.

    - `GX_CLOUD_ACCESS_TOKEN` - The user access token you generated previously.

    - `GX_CLOUD_SNOWFLAKE_PASSWORD` - The password you use to access your Snowflake account.

    To set the environment variables, see the documentation specific to your operating system.

2. In Jupyter Notebook, run the following code to start the GX Cloud Agent:
    
    ```python title="Jupyter Notebook"
       gx-agent
    ```

## Create the Snowflake Data Asset

1. In GX Cloud, click **Data Assets** > **New Asset**.

2. Complete the following mandatory fields:

    - **Datasource name**: Enter a meaningful name for the Data Asset.

    - **Username**: Enter your Snowflake username.

    - **Password variable**: Enter `GX_CLOUD_SNOWFLAKE_PASSWORD`.

    - **Account or locator**: Enter your Snowflake account or locator information.

3. (Optional) Complete the following fields:

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored.
 
    - **Schema**: Enter the name of the schema for the Snowflake database here the data you want to validate is stored.

    - **Warehouse**: Enter the name of the Snowflake database warehouse.

    - **Role**: Enter your Snowflake role.

    - **Authenticator**: Enter the Snowflake database authenticator. 

4. (Optional) Clear **Create temp table** if you don't want to create a temporary database table.

5. (Optional) Clear **Test connection** if you don't want to test the Data Asset connection.

6. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Asset name**: Enter a name for the Data Asset.

    - **Table name** (Optional) : When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.

    - **Query**: Enter a query. 

7. (Optional) Select **Add table/query** to add additional tables or queries and repeat step 6.

8. Click **Finish**.

## Add Expectations











