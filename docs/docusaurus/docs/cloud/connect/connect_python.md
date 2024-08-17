---
sidebar_label: 'Connect to GX Cloud with Python'
title: 'Connect to GX Cloud with Python'
id: connect_python
description: Connect to a GX Cloud account and validate data from a Python script.
---

Learn how to use GX Cloud from a Python script or interpreter, such as a Jupyter Notebook. You'll install Great Expectations, configure your GX Cloud environment variables, connect to sample data, build your first Expectation, validate data, and review the validation results through Python code.

## Prerequisites

- You have internet access and download permissions.
- You have a [GX Cloud account](https://greatexpectations.io/cloud).

## Prepare your environment

1. Download and install Python. See [Active Python Releases](https://www.python.org/downloads/).

2. Download and install pip. See the [pip documentation](https://pip.pypa.io/en/stable/cli/pip/).


## Install GX

1. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete.

## Get your user access token and organization ID

You'll need your user access token and organization ID to set your environment variables. Don't commit your access tokens to your version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

## Set the GX Cloud Organization ID and user access token as environment variables

Environment variables securely store your GX Cloud access credentials.

1. Save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZATION_ID** as environment variables by entering `export ENV_VAR_NAME=env_var_value` in the terminal or adding the command to your `~/.bashrc` or `~/.zshrc` file. For example:

    ```bash title="Terminal input"
    export GX_CLOUD_ACCESS_TOKEN=<user_access_token>
    export GX_CLOUD_ORGANIZATION_ID=<organization_id>
    ```

    :::note
   After you save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZTION_ID**, you can use Python scripts to access GX Cloud and complete other tasks. See the [GX OSS guides](/core/introduction/gx_overview.md).
    :::

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

## Create a Data Context

- Run the following Python code to create a Data Context object:

    ```python title="Python" name="tutorials/quickstart/quickstart.py get_context"
    ```
  
    The Data Context will detect the previously set environment variables and connect to your GX Cloud account.

## Connect to a Data Asset

- Run the following Python code to connect to existing `.csv` data stored in the `great_expectations` GitHub repository and create a Validator object:

    ```python title="Python" name="tutorials/quickstart/quickstart.py connect_to_data"
    ```

    The code example uses the default Data Source for Pandas to access the `.csv` data from the file at the specified URL path.

## Create Expectations

- Run the following Python code to create two Expectations and save them to the Expectation Suite:

    ```python title="Python" name="tutorials/quickstart/quickstart.py create_expectation"
    ```

  The first Expectation uses domain knowledge (the `pickup_datetime` shouldn't be null).

  The second Expectation uses explicit kwargs along with the `passenger_count` column.

## Validate data

1. Run the following Python code to define a Checkpoint and examine the data to determine if it matches the defined Expectations:

    ```python title="Python" name="tutorials/quickstart/quickstart.py create_checkpoint"
    ```

2. Use the following command to return the Validation Results:

    ```python title="Python" name="tutorials/quickstart/quickstart.py run_checkpoint"
    ```

3. Run the following Python code to view an HTML representation of the Validation Results in the generated Data Docs:

    ```python title="Python" name="tutorials/quickstart/quickstart.py view_results"
    ```
