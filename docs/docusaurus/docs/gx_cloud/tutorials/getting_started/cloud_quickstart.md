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

## Introduction

Few things are as daunting as taking your first steps with a new piece of software. This guide will introduce you to GX Cloud and demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of connecting to your data, building your first Expectation based off of an initial Batch of that data, validating your data with that Expectation, and finally reviewing the results of your validation.

Once you have completed this guide you will have a foundation in the basics of using GX Cloud. In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

<Prerequisites>

- Installed Great Expectations OSS on your machine.
- Followed invitation email instructions from the GX team after signing up for Early Access.
- Successfully logged in to GX Cloud at [https://app.greatexpectations.io](https://app.greatexpectations.io).
- A passion for data quality.

</Prerequisites> 

## Overview

With GX Cloud you can get up and running within just a few minutes. The data we're going to use for this overview is the [NYC taxi data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This is an open data set which is updated every month. Each record in the data corresponds to one taxi ride. Use individual steps instructions in order to connect to your data.
The full process you'll be using will look like:

```python title="Jupyter Notebook"
import great_expectations as gx
import pandas as pd


# Create Data Context
context = gx.get_context(
    cloud_access_token = "<user_token_you_generated_in_the_app>",
    cloud_organization_id = "<organization_id_from_the_app>",
)


# Connect to data
datasource_name = "my_quickstart_datasource"
data_connector_name = "default_runtime_data_connector"
batch_id = "batch_id"

datasource_yaml = f"""
  name: {datasource_name}
  class_name: Datasource
  execution_engine:
      class_name: PandasExecutionEngine
  data_connectors:
      {data_connector_name}:
          class_name: RuntimeDataConnector
          batch_identifiers:
          - {batch_id}
  """

datasource = context.test_yaml_config(datasource_yaml)
datasource = context.save_datasource(datasource)


# Create Expectation Suite
expectation_suite = context.create_expectation_suite(
    expectation_suite_name="my_quickstart_exp_suite"
)


# Add Expectation
expectation_configuration = gx.core.ExpectationConfiguration(**{
  "expectation_type": "expect_column_values_to_not_be_null",
  "kwargs": {
    "column": "trip_distance",
  },
  "meta":{}
})

expectation_suite.add_expectation(
    expectation_configuration=expectation_configuration
)
print(expectation_suite)
context.save_expectation_suite(expectation_suite)


# Validate data
checkpoint_name = "my_quickstart_checkpoint"
checkpoint_config = {
  "name": checkpoint_name,
  "validations": [{
      "expectation_suite_name": expectation_suite.expectation_suite_name,
      "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
      "batch_request": {
          "datasource_name": datasource_name,
          "data_connector_name": data_connector_name,
          "data_asset_name": "my_quickstart_table",
      },
  }],
  "config_version": 1,
  "class_name": "Checkpoint"
}

context.add_or_update_checkpoint(**checkpoint_config)
checkpoint = context.get_checkpoint(checkpoint_name)
print(checkpoint)


# View results
path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
df = pd.read_csv(path_to_data)

batch_request = {
    "runtime_parameters": {
        "batch_data": df
    },
    "batch_identifiers": {
        batch_id: "jan_2019"
    }
}

result = context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id, batch_request=batch_request)
print(result)
```

In the following steps we'll break down exactly what is happening here so that you can follow along and perform a <TechnicalTag tag="validation" text="Validation"/> yourself.

## Steps

### 1. Setup

#### 1.1 Generate User Token

Go to [“Settings” > “Tokens”](https://app.greatexpectations.io/tokens) in the navigation panel and generate a User Token. 
These tokens are see-once and stored as a hash in Great Expectation Cloud's backend database. Once you copy API key and close the dialog, the Cloud UI will never show the token value again. 

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
Please note that `cloud_access_token` is sensitive information and should not be committed to version control software. Alternatively, add these as [Data Context config variables](docs/guides/setup/configuring_data_contexts/how_to_configure_credentials.md)
:::

```python title="Jupyter Notebook"
os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id_from_the_app>"
os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<user_token_you_generated_in_the_app>"

context = gx.get_context()
```

### 2. Connect to data

Modify the following yaml code to connect to your <TechnicalTag tag="datasource" text="Datasource"/>

:::caution
Please note you should not include sensitive info/credentials directly in the config while connecting to your Datasource, since this would be persisted in plain text in the database and presented in Cloud UI. If credentials/full connection string is required, you should use [config variables file](docs/guides/setup/configuring_data_contexts/how_to_configure_credentials.md).
:::

```python title="Jupyter Notebook"
datasource_name = None
assert datasource_name is not None, "Please set datasource_name."
batch_id = None # batch_id is intended to help identify batches of data passed in directly through dataframes
assert batch_id is not None, "Please set batch_id."
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
          - {batch_id}
  """

# Test your configuration:
datasource = context.test_yaml_config(datasource_yaml)

# Save your datasource:
datasource = context.save_datasource(datasource)

# Confirm the datasource has been saved (Optional):
existing_datasource = context.get_datasource(datasource_name=datasource.name)
print(existing_datasource.config)
```

In case you need more details on how to connect to your specific data system, we have step by step how-to guides that cover many common cases. [Start here](/docs/guides/connecting_to_your_data/connect_to_data_overview.md)

### 3. Create Expectations

#### 3.1 Create Expectation Suite

An <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is a collection of verifiable assertions about data. Run this snippet to create a new, empty <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>:

```python title="Jupyter Notebook"
expectation_suite_name = None
assert expectation_suite_name is not None, "Please set expectation_suite_name."

expectation_suite = context.create_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
```

#### 3.2 Add Expectation

Modify and run this snippet to add an <TechnicalTag tag="expectation" text="Expectation"/> to the <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> you just created:

```python title="Jupyter Notebook"
# Get an existing Expectation Suite
expectation_suite_id = expectation_suite.ge_cloud_id
expectation_suite = context.get_expectation_suite(ge_cloud_id=expectation_suite_id)

# Look up all expectations types here - https://greatexpectations.io/expectations/
expectation_configuration = gx.core.ExpectationConfiguration(**{
  "expectation_type": "expect_column_values_to_not_be_null",
  "kwargs": {
    "column": None, # Enter your column name here
  },
  "meta":{}
})

expectation_suite.add_expectation(
    expectation_configuration=expectation_configuration
)
print(expectation_suite)

# Save the Expectation Suite
context.save_expectation_suite(expectation_suite)
```

With the Expectation defined above, we are stating that we _expect_ the column of your choice to always be populated. That is: none of the column's values should be null.


### 4. Validate data

#### 4.1 Create Checkpoint

Now that we have connected to data and defined an <TechnicalTag tag="expectation" text="Expectation"/>, it is time to validate whether our data meets the Expectation. To do this, we define a <TechnicalTag tag="checkpoint" text="Checkpoint"/>, which will allow us to repeat the <TechnicalTag tag="validation" text="Validation"/> in the future.

```python title="Jupyter Notebook"
checkpoint_name = None  # name your checkpoint here

# uncomment the lines below after successfully creating your Checkpoint to run this code again!
# checkpoint = context.get_checkpoint(checkpoint_name)
# checkpoint_id = checkpoint.ge_cloud_id

checkpoint_config = {
  # "id": checkpoint_id,  # uncomment after successfully creating your Checkpoint
  "name": checkpoint_name,
  "validations": [{
      "expectation_suite_name": expectation_suite_name,
      "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
      "batch_request": {
          "datasource_name": datasource_name,
          "data_connector_name": data_connector_name,
          "data_asset_name": "SET YOUR DATA ASSET NAME HERE",
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
path_to_data = None
assert path_to_data is not None, "Please set path_to_data. This can be a local filepath or a remote URL."
df = pd.read_csv(path_to_data)
batch_identifier = None
assert batch_identifier is not None, "Please set batch_identifier."

batch_request = {
    "runtime_parameters": {
        "batch_data": df
    },
    "batch_identifiers": {
        batch_id: batch_identifier
    }
}

result = context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id, batch_request=batch_request)
print(result)
```

#### 4.3 Review your results

After you run the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, you should see a link that takes you directly to GX Cloud, so you can see your <TechnicalTag tag="expectation" text="Expectations"/> and <TechnicalTag tag="validation_result" text="Validation Results"/> in the GX Cloud UI. 

Alternatively, you can visit the [Checkpoints page](https://app.greatexpectations.io/checkpoints) and filter by the Checkpoint, Expectation Suite, or Data Asset you want to see the results for.


## Next Steps 

Now that you've seen how to implement the GX workflow, it is time to customize the workflow to suit your specific use cases! To help with this we have prepared more detailed guides tailored to specific environments and resources.

To invite additional team members to the app visit [“Settings” > “Users”](https://app.greatexpectations.io/users).

For more details on installing GX for use with local filesystems, please see:

<SetupAndInstallForFilesystemData />

For guides on installing GX for use with cloud storage systems, please reference:

<SetupAndInstallForCloudData />

For information on installing GX for use with SQL databases, see:

<SetupAndInstallForSqlData />

And for instructions on installing GX for use with hosted data systems, read:

<SetupAndInstallForHostedData />
