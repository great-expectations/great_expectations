---
sidebar_label: 'Quickstart for GX Cloud and Snowflake'
title: 'Quickstart for GX Cloud and Snowflake'
id: snowflake_quickstart
description: Connect Great Expectations to Snowflake Data Assets.
---

In this quickstart, you'll learn how to connect GX Cloud to Snowflake Data Assets.

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

## Set the environment variables and start the GX Cloud Agent

1. Set the following environment variables:

    - `GX_CLOUD_ORGANIZATION_ID` - The Organization ID you copied previously.

    - `GX_CLOUD_ACCESS_TOKEN` - The user access token you generated previously.

    - `GX_CLOUD_SNOWFLAKE_PASSWORD` - The password you use to access your Snowflake account.

    To set the environment variables, see the documentation specific to your operating system. 

2. In Jupyter Notebook, run the following code to start the GX Cloud Agent:
    
    ```python title="Jupyter Notebook"
       gx-agent
    ```

    If you stop the GX Cloud Agent, close the terminal, and open a new terminal you'll need to set the environment variables again.

    To edit an environment variable, stop the GX Cloud Agent, edit the environment variable, save the change, and then restart the GX Cloud Agent.

## Create the Snowflake Data Asset

1. In GX Cloud, click **Data Assets** > **New Asset**.

2. Complete the following mandatory fields:

    - **Datasource name**: Enter a meaningful name for the Data Asset.

    - **Username**: Enter your Snowflake username.

    - **Password variable**: Enter `GX_CLOUD_SNOWFLAKE_PASSWORD`.

    - **Account or locator**: Enter your Snowflake account or locator information. The locator value must include the geographical region. For example, `us-east-1`. To locate these values see [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

3. Optional. Complete the following fields:

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored.
 
    - **Schema**: Enter the name of the schema for the Snowflake database where the data you want to validate is stored.

    - **Warehouse**: Enter the name of the Snowflake database warehouse.

    - **Role**: Enter your Snowflake role.

    - **Authenticator**: Enter the Snowflake database authenticator that you want to use to verify your Snowflake connection. 

4. Optional. Clear **Create temp table** if you don't want to create a temporary database table.

5. Optional. Clear **Test connection** if you don't want to test the Data Asset connection.

6. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Asset name**: Enter a name for the Data Asset.

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.

    - **Query**: Enter the query that you want to run on the table. 

7. Optional. Select **Add table/query** to add additional tables or queries and repeat step 6.

8. Click **Finish**.

## Add an Expectation

1. In the **Data Assets** list, click the Snowflake Data Asset name.

2. Click **New Expectation**.

3. Select an Expectation type, enter the column name, and then complete the optional fields.

    If you prefer to work in a code editor, click the **JSON Editor** tab and define your Expectation parameters in the code pane.

4. Click **Save**.

5. Optional. Repeat steps 1 to 4 to add additional Expectations.

## Validate Expectations

1. Click **Validate**.

2. When the confirmation message appears, click **See results**, or click the **Validations** tab.

