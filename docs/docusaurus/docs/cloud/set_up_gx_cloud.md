---
sidebar_label: 'Set up GX Cloud'
title: 'Set up GX Cloud'
description: Set up GX Cloud.
---

To get the most out of GX Cloud, you'll need to request a GX Cloud Beta account, install GX Cloud, generate and record access tokens, set environment variables, and then start the GX Cloud agent.

## Request a GX Cloud Beta account

1. Go to the GX Cloud Beta [sign-up page](https://greatexpectations.io/cloud).

2. Complete the sign-up form and then click **Submit**.

3. If you're invited to view a GX Cloud demonstration, click **Schedule a time to connect here** in the confirmation email to schedule a time to meet with a Great Expectations (GX) representative. After your meeting you'll be sent an email invitation to join your GX Cloud organization.

4. In your GX Cloud email invitation, click **click here** to set your password and sign in.

5. Enter your email address and password, and then click **Continue to GX Cloud**. Alternatively, click **Log in with Google** and use your Google credentials to access GX Cloud.


## Prepare your environment

1. Download and install Python. See [Active Python Releases](https://www.python.org/downloads/).

2. Download and install pip. See the [pip documentation](https://pip.pypa.io/en/stable/cli/pip/).

3. Run the following command in an empty base directory inside a Python virtual environment to install GX Cloud and its dependencies:

    ```bash title="Terminal input"
    pip install 'great_expectations[cloud,snowflake]'
    ```

    It can take several minutes for the installation to complete.

    If you've previously installed GX Cloud, run the following command to upgrade to the latest version:

    ```bash title="Terminal input"
    pip install 'great_expectations_cloud[snowflake]' --upgrade
    ```

## Get your user access token and organization ID

You'll need your user access token and organization ID to set your environment variables. Don't commit your access tokens to your version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **Access tokens** pane, click **Create user access token**.

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Admin**. For more information about the available roles, click **Information** (?) .

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

## Set the environment variables and start the GX Cloud agent

Environment variables securely store your GX Cloud access credentials. The GX Cloud agent runs open source GX code in GX Cloud, and it allows you to securely access your data without connecting to it or interacting with it directly. 

1. Set the following environment variables:

    - `GX_CLOUD_ACCESS_TOKEN` - The user access token you generated previously.
    
    - `GX_CLOUD_ORGANIZATION_ID` - The Organization ID you copied previously.

    To set the environment variables, see the documentation specific to your operating system. 

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

3. Run the following code to start the GX Cloud agent:
    
    ```bash title="Terminal input"
    gx-agent
    ```

    If you stop the GX Cloud agent, close the terminal, and open a new terminal you'll need to set the environment variables again.

    To edit an environment variable, stop the GX Cloud agent, edit the environment variable, save the change, and then restart the GX Cloud agent.

## Troubleshoot

Use the following information to help resolve issues with the GX Cloud agent installation.

**Error: `great_expectations.agent.agent.GXAgentError: Missing or badly formed environment variable`**

Confirm the `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID` environment variables are set and available in your local environment. For example, on Unix systems, you can run `echo $GX_CLOUD_ORGANIZATION_ID` to confirm that your organization ID is present and set.

**Error: `pika not found`**

Run the following command:

```bash title="Terminal input"
    pip install great_expectations[cloud]
```
