---
title: Quickstart with GX Cloud
tag: [tutorial, getting started, quickstart, cloud]
---
# Quickstart with Great Expectations Cloud

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import SetupAndInstallForFilesystemData from '/docs/components/setup/link_lists/_setup_and_install_for_filesystem_data.md'
import SetupAndInstallForHostedData from '/docs/components/setup/link_lists/_setup_and_install_for_hosted_data.md'
import SetupAndInstallForCloudData from '/docs/components/setup/link_lists/_setup_and_install_for_cloud_data.md'
import Prerequisites from '/docs/components/_prerequisites.jsx'

Few things are as daunting as taking your first steps with a new piece of software. This guide will introduce you to GX Cloud and demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of connecting to your data, building your first Expectation based off of an initial Batch of that data, validating your data with that Expectation, and finally reviewing the results of your validation.

Once you have completed this guide you will have a foundation in the basics of using GX Cloud. In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

<Prerequisites>

- Installed Great Expectations OSS on your machine.
- Followed invitation email instructions from the GX team after signing up for Early Access.
- Successfully logged in to GX Cloud at [https://app.greatexpectations.io](https://app.greatexpectations.io).
- A passion for data quality.

</Prerequisites>

## Steps

### 1. Setup

#### 1.1 Generate access token

Go to [“Settings” > “Tokens”](https://app.greatexpectations.io/tokens) in the navigation panel and generate an access token. Both `admin` and `editor` roles will suffice for this guide.
These tokens are view-once and stored as a hash in Great Expectation Cloud's backend database. Once you copy the API key and close the dialog, the Cloud UI will never show the token value again.

#### 1.2 Import modules

:::tip
Any Python Interpreter or script file will work for the remaining steps in the guide, we recommend using a Jupyter Notebook, since they are included in the OSS GX installation and give the best experience of both composing a script file and running code in a live interpreter.
:::

Switch to Jupyter Notebook and import modules we're going to use in this tutorial.

```python title="Jupyter Notebook"
import great_expectations as gx
import pandas as pd
import os
```

#### 1.3 Create Data Context

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

### 2. Create Datasource

Modify the following snippet code to connect to your <TechnicalTag tag="datasource" text="Datasource"/>.
In case you don't have some data handy to test in this guide, we can use the [NYC taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This is an open data set which is updated every month. Each record in the data corresponds to one taxi ride. You can find a link to it in the snippet below.

:::caution
Please note you should not include sensitive info/credentials directly in the config while connecting to your Datasource, since this would be persisted in plain text in the database and presented in Cloud UI. If credentials/full connection string is required, you should use a [config variables file](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials/).
:::

```python title="Jupyter Notebook"
datasource_name = None
assert datasource_name is not None, "Please set datasource_name."
batch_identifier_name = None # batch_identifier_name is intended to help identify batches of data passed in directly through dataframes
assert batch_identifier_name is not None, "Please set batch_identifier_name."
data_connector_name = None
assert data_connector_name is not None, "Please set data_connector_name."

datasource_yaml = f"""
  name: {datasource_name}
  class_name: Datasource
  execution_engine:
      class_name: PandasExecutionEngine
  data_connectors:
      {data_connector_name}:
          class_name: RuntimeDataConnector
          batch_identifiers:
          - {batch_identifier_name}
"""

path_to_data = None
# to use sample data uncomment next line
# path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
assert path_to_data is not None, "Please set path_to_data. This can be a local filepath or a remote URL."
df = pd.read_csv(path_to_data)
batch_identifier_value = None
assert batch_identifier_value is not None, "Please set batch_identifier."

batch_request = {
    "runtime_parameters": {
        "batch_data": df
    },
    "batch_identifiers": {
        batch_identifier_name: batch_identifier_value
    }
}

# Test your configuration:
datasource = context.test_yaml_config(datasource_yaml)

# Save your datasource:
datasource = context.save_datasource(datasource)
```

In case you need more details on how to connect to your specific data system, we have step by step how-to guides that cover many common cases. [Start here](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview)

### 3. Create Expectations

#### 3.1 Create Expectation Suite

An <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is a collection of verifiable assertions about data. Run this snippet to create a new, empty <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>:

```python title="Jupyter Notebook"
expectation_suite_name = None
assert expectation_suite_name is not None, "Please set expectation_suite_name."

expectation_suite = context.add_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
```

#### 3.2 Add Expectation

Modify and run this snippet to add an <TechnicalTag tag="expectation" text="Expectation"/> to the <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> you just created:

```python title="Jupyter Notebook"
# Get an existing Expectation Suite
expectation_suite_id = expectation_suite.ge_cloud_id
expectation_suite = context.get_expectation_suite(ge_cloud_id=expectation_suite_id)
column_name = None # set column name you want to test here
assert column_name is not None, "Please set column_name."

# Look up all expectations types here - https://greatexpectations.io/expectations/
expectation_configuration = gx.core.ExpectationConfiguration(**{
  "expectation_type": "expect_column_values_to_not_be_null",
  "kwargs": {
    "column": column_name,
  },
  "meta":{}
})

expectation_suite.add_expectation(
    expectation_configuration=expectation_configuration
)
print(expectation_suite)

# Save the Expectation Suite
context.save_expectation_suite(expectation_suite=expectation_suite)
```

With the Expectation defined above, we are stating that we _expect_ the column of your choice to always be populated. That is: none of the column's values should be null.


### 4. Validate data

#### 4.1 Create Checkpoint

Now that we have connected to data and defined an <TechnicalTag tag="expectation" text="Expectation"/>, it is time to validate whether our data meets the Expectation. To do this, we define a <TechnicalTag tag="checkpoint" text="Checkpoint"/>, which will allow us to repeat the <TechnicalTag tag="validation" text="Validation"/> in the future.

```python title="Jupyter Notebook"
checkpoint_name = None # name your checkpoint here
assert checkpoint_name is not None, "Please set checkpoint_name."
data_asset_name = None # name your table here
assert data_asset_name is not None, "Please set data_asset_name."

checkpoint_config = {
  "name": checkpoint_name,
  "validations": [{
      "expectation_suite_name": expectation_suite_name,
      "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
      "batch_request": {
          "datasource_name": datasource_name,
          "data_connector_name": data_connector_name,
          "data_asset_name": data_asset_name,
      },
  }],
  "config_version": 1,
  "class_name": "Checkpoint"
}

context.add_or_update_checkpoint(**checkpoint_config)
checkpoint = context.get_checkpoint(checkpoint_name)
print(checkpoint)
```

#### 4.2 Run Checkpoint

Once we have created the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, we will run it and get back the results from our <TechnicalTag tag="validation" text="Validation"/>.

```python title="Jupyter Notebook"
result = context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id, batch_request=batch_request)
print(result)
```

#### 4.3 Review your results

After you run the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, you should see a `validation_result_url` in the result, that takes you directly to GX Cloud, so you can see your <TechnicalTag tag="expectation" text="Expectations"/> and <TechnicalTag tag="validation_result" text="Validation Results"/> in the GX Cloud UI.

Alternatively, you can visit the [Checkpoints page](https://app.greatexpectations.io/checkpoints) and filter by the Checkpoint, Expectation Suite, or Data Asset you want to see the results for.


## Next Steps

Now that you've seen how to implement the GX workflow, it is time to customize the workflow to suit your specific use cases! To help with this we have prepared more detailed guides tailored to specific environments and resources.

To get all the snippets above in one script, visit [GX OSS repository](https://github.com/great-expectations/great_expectations/blob/develop/assets/scripts/gx_cloud/experimental/onboarding_script.py)

To invite additional team members to the app visit [“Settings” > “Users”](https://app.greatexpectations.io/users).

For more details on installing GX for use with local filesystems, please see:

<SetupAndInstallForFilesystemData />

For guides on installing GX for use with cloud storage systems, please reference:

<SetupAndInstallForCloudData />

For information on installing GX for use with SQL databases, see:

<SetupAndInstallForSqlData />

And for instructions on installing GX for use with hosted data systems, read:

<SetupAndInstallForHostedData />
