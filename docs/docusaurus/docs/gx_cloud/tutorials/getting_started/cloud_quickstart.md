---
title: Quickstart with GX Cloud
tag: [tutorial, getting started, quickstart, cloud]
---
# Quickstart with Great Expectations

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import SetupAndInstallForFilesystemData from '/docs/components/setup/link_lists/_setup_and_install_for_filesystem_data.md'
import SetupAndInstallForHostedData from '/docs/components/setup/link_lists/_setup_and_install_for_hosted_data.md'
import SetupAndInstallForCloudData from '/docs/components/setup/link_lists/_setup_and_install_for_cloud_data.md'

## Introduction

Few things are as daunting as taking your first steps with a new piece of software. This guide will introduce you to GX Cloud and demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of connecting to your data, building your first Expectation based off of an initial Batch of that data, validating your data with that Expectation, and finally reviewing the results of your validation.

Once you have completed this guide you will have a foundation in the basics of using GX Cloud. In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

:::note Prerequisites
- This tutorial assumes you can log in Great Expectations Cloud and have running Jupyter Notebook with set <TechnicalTag tag="data_context" text="Data Context"/>. If that's not the case please complete [Getting started with GX Cloud](/docs/gx_cloud/tutorials/getting_started/getting_started_with_gx_cloud.md) first.
:::


## Steps

### 1. Connect to data

Modify this config to connect to your datasource. Otherwise, for the purpose of this guide, you can use this simple Pandas datasource configuration to connect to from your Jupyter Notebook:

```python
datasource_yaml = f"""
  name: <NAME_OF_YOUR_DATASOURCE>
  class_name: Datasource
  execution_engine:
      class_name: PandasExecutionEngine
  data_connectors:
      default_runtime_data_connector:
          class_name: RuntimeDataConnector
          batch_identifiers:
          - my_identifier
  """

# Test your configuration (Optional):
datasource = context.test_yaml_config(datasource_yaml)

# Save your datasource:
datasource = context.save_datasource(datasource)

# Confirm the datasource has been saved (Optional):
existing_datasource = context.get_datasource(datasource_name=datasource.name)
print(existing_datasource.config)
```

In case you need more details on how to connect to your specific data system, we have step by step how-to guides that cover many common cases. [Start here](/docs/guides/connecting_to_your_data/connect_to_data_overview.md)

### 2. Create Expectations

#### 2.1 Create Expectation Suite

An <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is a collection of verifiable assertions about data. Paste this snippet into the next Jupyter Notebook cell to create it:

```python
expectation_suite = context.create_expectation_suite(
    expectation_suite_name=None # Enter your expectation suite name here
)
```

#### 2.2 Add Expectation

Modify and run this snippet to add an <TechnicalTag tag="expectation" text="Expectation"/> to <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> you just created:

```python
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


### 3. Validate data

#### 3.1 Create Checkpoint

Now that we have defined our <TechnicalTag tag="expectation" text="Expectation"/> it is time for GX to introspect our data and see if it corresponds to what we told GX to expect. To do this, we define a <TechnicalTag tag="checkpoint" text="Checkpoint"/> (which will allow us to repeat the <TechnicalTag tag="validation" text="Validation"/> in the future).

```python
# First, fetch the ExpectationSuite you will use to define a Validation
expectation_suite = context.get_expectation_suite(expectation_suite_name=None) # add your expectation suite name here 
checkpoint_name = None  # name your checkpoint here

# uncomment the lines below after successfully creating your Checkpoint to run this code again!
# checkpoint = context.get_checkpoint(checkpoint_name)
# checkpoint_id = checkpoint.ge_cloud_id

checkpoint_config = {
  # "id": checkpoint_id,  # uncomment after successfully creating your Checkpoint
  "name": checkpoint_name,
  "validations": [{
      "expectation_suite_name": expectation_suite.expectation_suite_name,
      "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
      "batch_request": {
          "datasource_name": "YOUR DATASOURCE NAME HERE",
          "data_connector_name": "YOUR DATA CONNECTOR NAME HERE",
          "data_asset_name": "YOUR DATA ASSET NAME HERE",
      },
  }],
  "config_version": 1,
  "class_name": "Checkpoint"
}

context.add_or_update_checkpoint(**checkpoint_config)
checkpoint = context.get_checkpoint(checkpoint_name)
print(checkpoint)
```
#### 3.2 Run Checkpoint

Once we have created the <TechnicalTag tag="checkpoint" text="Checkpoint"/>, we will run it and get back the results from our <TechnicalTag tag="validation" text="Validation"/>.

```python
import pandas as pd

df = pd.DataFrame(columns=range(8))
batch_request = {
    "runtime_parameters": {
        "batch_data": df
    },
    "batch_identifiers": {
        "my_identifier": "test"
    }
}

context.run_checkpoint(ge_cloud_id=checkpoint.ge_cloud_id, batch_request=batch_request)
```

#### 3.3 Review your results

Once you ran the <TechnicalTag tag="checkpoint" text="Checkpoint"/> you should see a link that takes you directly to GX Cloud, so you can see your <TechnicalTag tag="expectation" text="Expectations"/> and <TechnicalTag tag="validation_result" text="Validation Results"/> in the GX Cloud UI.


## Next Steps 

Now that you've seen how easy it is to implement the GX workflow, it is time to customize that workflow to suit your specific use cases! To help with this we have prepared some more detailed installation and setting up guides tailored to specific environments and resources.

For more details on installing GX for use with local filesystems, please see:

<SetupAndInstallForFilesystemData />

For guides on installing GX for use with cloud storage systems, please reference:

<SetupAndInstallForCloudData />

For information on installing GX for use with SQL databases, see:

<SetupAndInstallForSqlData />

And for instructions on installing GX for use with hosted data systems, read:

<SetupAndInstallForHostedData />
