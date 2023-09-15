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
import Prerequisites from '/docs/components/_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

If you're new to GX Cloud, the information provided here is intended to demonstrate GX Cloud features and functionality. You'll connect to a Datasource, build an Expectation using sample Batch data, validate data with the Expectation, and review validation results in a Data Doc.

After you've tested GX Cloud features and functionality, you can connect to your own data and create Expectations that are specific to your business needs. If you're already familiar with GX Cloud, or you prefer to use your own data for testing, see.

Although you can use any Python Interpreter or script file to run Python code, GX recommends using Jupyter Notebook. Jupyter Notebook is included with OSS GX and is the best option for composing script files and running code.

The example code is available in the [onboarding script repository](https://github.com/great-expectations/great_expectations/blob/develop/assets/scripts/gx_cloud/experimental/onboarding_script.py).

## Prerequisites

<Prerequisites>

- Followed invitation email instructions from the GX team after signing up for Early Access.
- Successfully logged in to GX Cloud at [https://app.greatexpectations.io](https://app.greatexpectations.io).

</Prerequisites>

## Install GX

1. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete.

2. In Jupyter Notebook, run the following Python code to import the `great_expectations` module:

    ```python name="tutorials/quickstart/quickstart.py import_gx"
    ```

## Generate a user access token

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the Access tokens pane, click {**Create user access token**.

3. Complete the following fields:

    - **Token name** - Enter a name for the token that will help you quickly identify it.

    - **Role** - Select **Viewer**, **Editor**, or **Admin**. For more information about these roles, click **?**.

4. Click **Create**.

5. Copy the access token and store it in a secure location. The token can't be retrieved after you close the dialog.

6. Click **Close**.

#### Import modules

In Jupyter Notebook, run the following Python code to import the modules you'll use to test functionality:

```python title="Jupyter Notebook"
import great_expectations as gx
import pandas as pd
import os
```

## Create a Data Context

Paste this snippet into the next notebook cell to instantiate Cloud <TechnicalTag tag="data_context" text="Data Context"/>.

:::caution
Please note that access tokens are sensitive information and should not be committed to version control software. Alternatively, add these as [Data Context config variables](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials/)
:::

```python title="Jupyter Notebook"
os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<your_gx_cloud_access_token>"
# your organization_id is indicated on https://app.greatexpectations.io/tokens page
os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id_from_the_app>"

context = gx.get_context()
```

## Connect to a Data Source

Modify the following snippet code to connect to your <TechnicalTag tag="datasource" text="Data Source"/>.
In case you don't have some data handy to test in this guide, we can use the [NYC taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This is an open data set which is updated every month. Each record in the data corresponds to one taxi ride. You can find a link to it in the snippet below.

:::caution
Please note you should not include sensitive info/credentials directly in the config while connecting to your Data Source, since this would be persisted in plain text in the database and presented in Cloud UI. If credentials/full connection string is required, you should use a [config variables file](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials/).
:::

```python title="Jupyter Notebook"
# Give your datasource a name
datasource_name = None
datasource = context.sources.add_pandas(datasource_name)

# Give your first Asset a name
asset_name = None
path_to_data = None
# to use sample data uncomment next line
# path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)

# Build batch request
batch_request = asset.build_batch_request()
```

In case you need more details on how to connect to your specific data system, we have step by step how-to guides that cover many common cases. [Start here](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview)

## Create an Expectation Suite

An <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is a collection of verifiable assertions about data. Run this snippet to create a new, empty <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>:

```python title="Jupyter Notebook"
expectation_suite_name = None
assert expectation_suite_name is not None, "Please set expectation_suite_name."

expectation_suite = context.add_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
```

## Add an Expectation to an Expectation Suite

Modify and run this snippet to add an <TechnicalTag tag="expectation" text="Expectation"/> to the <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> you just created:

```python title="Jupyter Notebook"
# Get an existing Expectation Suite
expectation_suite_id = expectation_suite.ge_cloud_id
expectation_suite = context.get_expectation_suite(ge_cloud_id=expectation_suite_id)
column_name = None # set column name you want to test here
assert column_name is not None, "Please set column_name."

# Look up all expectations types here - https://greatexpectations.io/expectations/
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

# Save the Expectation Suite
context.save_expectation_suite(expectation_suite=expectation_suite)
```

With the Expectation defined above, we are stating that we _expect_ the column of your choice to always be populated. That is: none of the column's values should be null.


## Create and run Checkpoint

Now that we have connected to data and defined an <TechnicalTag tag="expectation" text="Expectation"/>, it is time to validate whether our data meets the Expectation. To do this, we define a <TechnicalTag tag="checkpoint" text="Checkpoint"/>, which will allow us to repeat the <TechnicalTag tag="validation" text="Validation"/> in the future.

Once we have created the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, we will run it and get back the results from our <TechnicalTag tag="validation" text="Validation"/>.

```python title="Jupyter Notebook"
checkpoint_name = None # name your checkpoint here
assert checkpoint_name is not None, "Please set checkpoint_name."

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

After you run the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, you should see a `validation_result_url` in the result, that takes you directly to GX Cloud, so you can see your <TechnicalTag tag="expectation" text="Expectations"/> and <TechnicalTag tag="validation_result" text="Validation Results"/> in the GX Cloud UI.

Alternatively, you can visit the [Checkpoints page](https://app.greatexpectations.io/checkpoints) and filter by the Checkpoint, Expectation Suite, or Data Asset you want to see the results for.


## Add a Slack notifications (Optional)

Add the `send_slack_notification_on_validation_result` Action to the <TechnicalTag tag="checkpoint" text="Checkpoint" /> configuration.

<Tabs
  groupId="webhook-or-app-python"
  defaultValue='webhook'
  values={[
  {label: 'For Webhook', value:'webhook'},
  {label: 'For App', value:'app'},
  ]}>

<TabItem value="webhook">

### Webhook config

```python title="Jupyter Notebook"
slack_webhook = None # put the actual webhook URL
assert slack_webhook is not None, "Please set slack_webhook."

checkpoint_config = {
    ...
    "action_list": [
        {
            "name": "send_slack_notification_on_validation_result", # name can be set to any value
            "action": {
                "class_name": "SlackNotificationAction",
                "slack_webhook": slack_webhook,
                "notify_on": "all", # possible values: "all", "failure", "success"
                "renderer": {
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            },
        },
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction",
            }
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction",
            }
        },
    ],
}
```

</TabItem>

<TabItem value="app">

### Slack bot config

```python title="Jupyter Notebook"
bot_token = None # put the actual bot token
assert bot_token is not None, "Please set bot_token."
channel_name = None # put the actual Slack channel name
assert channel_name is not None, "Please set channel_name."

checkpoint_config = {
    ...
    "action_list": [
        {
            "name": "send_slack_notification_on_validation_result", # name can be set to any value
            "action": {
                "class_name": "SlackNotificationAction",
                "slack_token": bot_token,
                "slack_channel": channel_name,
                "notify_on": "all", # possible values: "all", "failure", "success"
                "renderer": {
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            },
        },
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction",
            }
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction",
            }
        },
    ],
}
```

</TabItem>

</Tabs>

Run your <TechnicalTag tag="checkpoint" text="Checkpoint" /> to validate a <TechnicalTag tag="batch" text="Batch"/> of data and receive Slack notification on the success or failure of the <TechnicalTag tag="expectation_suite" text="Expectation Suite's"/> <TechnicalTag tag="validation" text="Validation"/>. 
Find additional information [here](https://docs.greatexpectations.io/docs/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action/)

## Next Steps

Invite team members to GX Cloud [“Settings” > “Users”](https://app.greatexpectations.io/users).

For more details on installing GX for use with local filesystems, please see:

<SetupAndInstallForFilesystemData />

For guides on installing GX for use with cloud storage systems, please reference:

<SetupAndInstallForCloudData />

For information on installing GX for use with SQL databases, see:

<SetupAndInstallForSqlData />

Install GX for use with hosted data systems, read:

<SetupAndInstallForHostedData />