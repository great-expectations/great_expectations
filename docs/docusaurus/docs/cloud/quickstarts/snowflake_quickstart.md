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

## Set the environment variables and run the GX CLoud Agent

1. Set the following environment variables:

    - GX_CLOUD_ORGANIZATION_ID - Set this value to the Organization ID you copied previously.

    - GX_CLOUD_ACCESS_TOKEN - Set this value to the user access token you generated previously.

    - GX_CLOUD_SNOWFLAKE_PASSWORD - Set this value to the password you use to access your Snowflake account.

    To set the environment variables, see the documentation specific to your operating system.

2. In Jupyter Notebook, run the following code to start the GX Cloud Agent:







