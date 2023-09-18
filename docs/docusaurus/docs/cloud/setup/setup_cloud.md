---
sidebar_label: Try GX Cloud
title: Try GX Cloud
id: setup_cloud
description: An end-to-end reference to help new users set up GX Cloud.
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import SetupAndInstallForFilesystemData from '/docs/components/setup/link_lists/_setup_and_install_for_filesystem_data.md'
import SetupAndInstallForHostedData from '/docs/components/setup/link_lists/_setup_and_install_for_hosted_data.md'
import SetupAndInstallForCloudData from '/docs/components/setup/link_lists/_setup_and_install_for_cloud_data.md'

If you're new to GX Cloud, the information provided here is intended to demonstrate GX Cloud features and functionality. You'll connect to a Datasource, build an Expectation using sample Batch data, validate data with the Expectation, and review validation results in a Data Doc.

After you've tested GX Cloud features and functionality, you can connect to your own data and create Expectations that are specific to your business needs. If you're already familiar with GX Cloud, or you prefer to use your own data for testing, see.

Although you can use any Python Interpreter or script file to run Python code, GX recommends using Jupyter Notebook. Jupyter Notebook is included with OSS GX and is the best option for composing script files and running code.

The example code is available in the [onboarding script repository](https://github.com/great-expectations/great_expectations/blob/develop/assets/scripts/gx_cloud/experimental/onboarding_script.py).

## Prerequisites

- An active [GX Cloud Beta Account](https://greatexpectations.io/cloud).

## Prepare your environment

1. Download and install pip. See the [pip documentation](https://pip.pypa.io/en/stable/cli/pip/).

2. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete.

    If you've previously installed GX, run the following command to upgrade to the latest version:

    ```bash title="Terminal input"
    pip install great_expectations --upgrade
    ```
3. In Jupyter Notebook, run the following Python code to import the modules you'll use to test functionality:

    ```python title="Jupyter Notebook"
    import great_expectations as gx
    import pandas as pd
    import os
    ```

## Generate a user access token

You'll need your user and organization access tokens when you connect to a Datasource. Access tokens shouldn't be committed to version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the Access tokens pane, click **Create user access token**.

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Viewer**, **Editor**, or **Admin**. For more information about these roles, click **?**.

4. Click **Create**.

5. Copy the access token and store it in a secure location. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Locate and copy your organization ID:

    - Click **Settings** > **Tokens**.

    - Copy the value in the **Organization ID** field and save it in the same location with your user access token.


## Create a Data Context

1. In Jupyter Notebook, copy this code into a cell to instantiate the GX Cloud <TechnicalTag tag="data_context" text="Data Context"/>.

    ```python title="Jupyter Notebook"
    os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<user_access_token>"
    os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id>"
    context = gx.get_context()
    ```
2. Replace `user_access_token` and `organization_id` with the values you created and saved previously. See [Generate a user access token](#generate-a-user-access-token).

3. Run the code.

## Connect to a Datasource

In Jupyter Notebook, run the following code to connect to existing `.csv` NYC taxi trip data stored in the `great_expectations` GitHub repository:

```python title="Jupyter Notebook"
datasource_name = "Test"
datasource = context.sources.add_pandas(datasource_name)
asset_name = "Test"
path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)
batch_request = asset.build_batch_request()
```

## Create an Expectation Suite

An Expectation Suite is a collection of verifiable assertions about data. 

In Jupyter Notebook, run the following code to create a new, empty Expectation Suite:

```python title="Jupyter Notebook"
expectation_suite_name = "Test"
assert expectation_suite_name is not None, "Please set expectation_suite_name."

expectation_suite = context.add_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
```

## Add an Expectation to an Expectation Suite

1. In Jupyter Notebook, run the following code to add an Expectation to the Test Expectation Suite and display the Expectation settings:

    ```python title="Jupyter Notebook"
    expectation_suite_id = expectation_suite.ge_cloud_id
    expectation_suite = context.get_expectation_suite(ge_cloud_id=expectation_suite_id)
    column_name = "vendor_id"
    expectation_configuration = gx.core.ExpectationConfiguration(**{
    "expectation_type": "expect_column_min_to_be_between",
    "kwargs": {
        "column": column_name,
        "min_value": 0.1
    },
    "meta":{},
    })
    expectation_suite.add_expectation(
        expectation_configuration=expectation_configuration
    )
    print(expectation_suite)
    ```
2. Run the following code to save the Expectation Suite:

    ```python title="Jupyter Notebook"
    context.save_expectation_suite(expectation_suite=expectation_suite)
    ```
## Create and run a Checkpoint

In Jupyter Notebook, run the following code to create and then run a Checkpoint to validate the data meets the defined Expectation.

```python title="Jupyter Notebook"
checkpoint_name = "Test"
checkpoint_config = {
  "name": checkpoint_name,
  "validations": [{
      "expectation_suite_name": expectation_suite_name,
      "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
      "batch_request": {
          "datasource_name": datasource.name,
          "data_asset_name": asset.name,
      },
  }],
  "config_version": 1,
  "class_name": "Checkpoint"
}

context.add_or_update_checkpoint(**checkpoint_config)
checkpoint = context.get_checkpoint(checkpoint_name)

checkpoint.run()
```

## Review Validation Results

1. In GX Cloud, click **Checkpoints**.

2. Click the **Test** Checkpoint to view the Validation Results.
