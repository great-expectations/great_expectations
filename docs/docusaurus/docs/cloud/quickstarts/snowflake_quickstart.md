---
sidebar_label: 'Quickstart for GX Cloud and Snowflake'
title: 'Quickstart for GX Cloud and Snowflake'
id: snowflake_quickstart
description: Connect GX Cloud to Snowflake Data Assets.
---

In this quickstart, you'll learn how to connect GX Cloud to Snowflake Data Assets.

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you know your password.

- You have a [Docker instance](https://docs.docker.com/get-docker/).

## Generate your user access token and copy your organization ID

You'll need your user access token and organization ID to set your environment variables. Access tokens shouldn't be committed to version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **Access tokens** pane, click **Create user access token**.

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Admin**. For more information about the available roles, click **Information** (?).

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

## Set the environment variables and start the GX Cloud agent

Environment variables securely store your GX Cloud and Snowflake access credentials. The GX Cloud agent runs open source GX code in GX Cloud, and it allows you to securely access your data without connecting to it or interacting with it directly. 

1. Start the Docker Engine.

2. Run the following code to set the `GX_CLOUD_ACCESS_TOKEN`, `GX_CLOUD_ORGANIZATION_ID`, and `GX_CLOUD_SNOWFLAKE_PASSWORD` environment variables, install GX Cloud and its dependencies, and start the GX Cloud agent:

    ```bash title="Terminal input"
    docker run --rm -e GX_CLOUD_ACCESS_TOKEN="<user_access_token>" -e GX_CLOUD_ORGANIZATION_ID="<organization_id>" -e GX_CLOUD_SNOWFLAKE_PASSWORD="<snowflake_password>" greatexpectations/agent
    ```
    Replace `user_access_token`, `organization_id`, and `snowflake_password` with your own values. 

3. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

4. Optional. Run `docker ps` or open Docker Desktop to confirm the agent is running.

    If you stop the GX Cloud agent, close the terminal, and open a new terminal you'll need to set the environment variables again.

    To edit an environment variable, stop the GX Cloud agent, edit the environment variable, save the change, and then restart the GX Cloud agent.

## Create the Snowflake Data Asset

Create a Data Asset to define the data you want GX Cloud to access within Snowflake. 

1. In GX Cloud, click **Data Assets** > **New Asset**.

2. Click the **New Data Source** tab and then select **Snowflake**.

3. Enter a meaningful name for the Data Asset in the **Data Source name** field.

4. Optional. To use a connection string to connect to a Data Source, click the **Use connection string** selector, enter a connection string, and then move to step 6. 

5. Complete the following fields:

    - **Username**: Enter your Snowflake username.

    - **Account identifier**: Enter your Snowflake account or locator information. The locator value must include the geographical region. For example, `us-east-1`. To locate these values see [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

    - **Password/environment variable**: Enter `${GX_CLOUD_SNOWFLAKE_PASSWORD}`. If you haven't set this variable, see [Set up GX Cloud](../set_up_gx_cloud.md).

    - **Database**: Enter the name of the Snowflake database where the data you want to validate is stored.
 
    - **Schema**: Enter the name of the schema for the Snowflake database where the data you want to validate is stored.

    - **Warehouse**: Enter the name of the Snowflake database warehouse.

    - **Role**: Enter your Snowflake role.

6. Optional. Make the following changes:

    - Clear **Create temp table** if you don't want to create a temporary database table. Temporary database tables store data temporarily and can improve performance by making queries run faster.

    - Select **Test connection** if you don't want to test the Data Asset connection. Testing the connection to the Data Asset is a preventative measure that makes sure the connection configuration is correct. This verification can help you avoid errors and can reduce troubleshooting downtime.

7. Click **Continue**.

8. Select **Table Asset** or **Query Asset** and complete the following fields:

    - **Table name**: When **Table Asset** is selected, enter a name for the table you're creating in the Data Asset.
    
    - **Data Asset name**: Enter a name for the Data Asset. Data Asset names must be unique. If you use the same name for multiple Data Assets, each Data Asset must be associated with a unique Data Source.

    - **Query**: When **Query Asset** is selected, enter the query that you want to run on the table. 

9. Select the **Complete Asset** tab to create Expectations and run validations on the entire Data Asset, or select the **Batches** tab to create Expectations and run validations on subsets of Data Asset records and then complete the following fields:

    - **Split Data Asset by** - Select **Year** to split Data Asset records by year, select **Year - Month** to split Data Asset records by year and month, or select **Year - Month - Day** to split Data Asset records by year, month, and day.

    - **Column of datetime type** - Enter the name of the column containing the date and time data.

10. Optional. Select **Add Data Asset** to add additional tables or queries and repeat steps 8 and 9.

11. Click **Finish**.

## Add an Expectation

An Expectation is a verifiable assertion about your data. They make implicit assumptions about your data explicit.

1. In the **Data Assets** list, click the Snowflake Data Asset name.

2. Click **New Expectation**.

3. Select an Expectation type, enter the column name, and then complete the optional fields.

    If you prefer to work in a code editor, click the **JSON Editor** tab and define your Expectation parameters in the code pane.

4. Click **Save**.

5. Optional. Repeat steps 1 to 4 to add additional Expectations.

## Validate Expectations

1. Click **Validate**.

2. When the confirmation message appears, click **See results**, or click the **Validations** tab.
