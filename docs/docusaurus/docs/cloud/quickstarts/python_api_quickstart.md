---
sidebar_label: 'Quickstart for GX Cloud and Python scripts'
title: 'Quickstart for GX Cloud and Python scripts'
id: python_quickstart
description: Connect to a GX Cloud account from a Python script.
---
import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrereqPython from '/docs/components/prerequisites/_python_version.md'
import SetupAndInstallGx from '/docs/components/setup/link_lists/_setup_and_install_gx.md'
import DataContextInitializeInstantiateSave from '/docs/components/setup/link_lists/_data_context_initialize_instatiate_save.md'

In this quickstart, you'll learn how to use GX Cloud from a Python script or interpreter, such as a Jupyter Notebook. You'll install Great Expectations, configure your GX Cloud environment variables, connect to sample data, build your first Expectation, validate data, and review the validation results through Python code.

## Data validation workflow

The following diagram illustrates the end-to-end GX data validation workflow that you'll implement with this quickstart. Click a workflow step to view the related content.

```mermaid
flowchart LR
%%{init: {"themeVariables": {"fontSize" : "24px"}}}%%

    1(Install\n<b>GX</b>) --> 2("Create a\n<b>Data Context</b>")

    2 --> 3

    3("Connect\nto data</b>")

    3 --> 4

    4("Create a\n<b>Validator</b>") --> 5("Create\n<b>Expectations</b>")

    5 --> 6

    6("Run a\n<b>Checkpoint</b>") --> 7("View\n<b>Validation Results</b>")

%% Link each workflow step to section on page.
click 1 "#install-gx"
click 2 "#create-a-data-context"
click 3 "#connect-to-data"
click 4 "#connect-to-data"
click 5 "#create-expectations"
click 6 "#validate-data"
click 7 "#validate-data"
```

## Prerequisites

- <PrereqPython />
- pip
- An internet browser
- A GX Cloud Beta Account


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

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Admin**. For more information about the available roles, click **Information** (?) .

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

## Set the GX Cloud Organization ID and user access token as environment variables

Environment variables securely store your GX Cloud access credentials. To use GX Cloud in Python scripts: 

1. Save your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZATION_ID** as environment variables by entering `export ENV_VAR_NAME=env_var_value` in the terminal or adding the command to your `~/.bashrc` or `~/.zshrc` file. For example:

    ```bash title="Terminal input"
    export GX_CLOUD_ACCESS_TOKEN=<user_access_token>
    export GX_CLOUD_ORGANIZATION_ID=<organization_id>
    ```

    :::note
    Once you have saved your **GX_CLOUD_ACCESS_TOKEN** and **GX_CLOUD_ORGANIZTION_ID** you will be able to access your GX Cloud account in Python scripts and environments while following the [GX OSS guides](/docs/oss/) or using the [GX API documentation](/docs/reference/api).
    :::

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

## Create a Data Context

- Use the following command in Python to create a <TechnicalTag tag="data_context" text="Data Context"/> object:

    ```python name="tutorials/quickstart/quickstart.py get_context"
    ```

## Connect to data

- Use the following command in Python to connect to existing `.csv` data stored in the `great_expectations` GitHub repository and create a <TechnicalTag tag="validator" text="Validator"/> object:

    ```python name="tutorials/quickstart/quickstart.py connect_to_data"
    ```

    The code example uses the default <TechnicalTag tag="data_context" text="Data Context"/> <TechnicalTag tag="datasource" text="Data Source"/> for Pandas to access the `.csv` data from the file at the specified URL path.

## Create Expectations

- Use the following commands to create two <TechnicalTag tag="expectation" text="Expectations"/> and save them to the <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>:

    ```python name="tutorials/quickstart/quickstart.py create_expectation"
    ```

  The first <TechnicalTag tag="expectation" text="Expectation"/> uses domain knowledge (the `pickup_datetime` shouldn't be null).

  The second <TechnicalTag tag="expectation" text="Expectation"/> uses explicit kwargs along with the `passenger_count` column.

## Validate data

1. Use the following command to define a <TechnicalTag tag="checkpoint" text="Checkpoint"/> and examine the data to determine if it matches the defined <TechnicalTag tag="expectation" text="Expectations"/>:

    ```python name="tutorials/quickstart/quickstart.py create_checkpoint"
    ```

2. Use the following command to return the <TechnicalTag tag="validation_result" text="Validation Results"/>:

    ```python name="tutorials/quickstart/quickstart.py run_checkpoint"
    ```

3. Use the following command to view an HTML representation of the <TechnicalTag tag="validation_result" text="Validation Results"/> in the generated <TechnicalTag tag="data_docs" text="Data Docs"/>:

    ```python name="tutorials/quickstart/quickstart.py view_results"
    ```

## Related documentation

If you're ready to continue your GX journey, the following topics can help you implement a solution for your specific environment and business requirements:

- [Install GX in a specific environment with support for a specific Data Source](/docs/oss/guides/setup/installation/install_gx).
- [Initialize, instantiate, and save a Data Context](/docs/oss/guides/setup/configure_data_contexts_lp).
- [Connect to Data Sources](/docs/oss/guides/connecting_to_your_data/connect_to_data_lp).
- [Create and manage Expectations and Expectation Suites](/docs/oss/guides/expectations/expectations_lp/).
- [Create, manage, and run Checkpoints](/docs/oss/guides/validation/checkpoints/checkpoint_lp/).