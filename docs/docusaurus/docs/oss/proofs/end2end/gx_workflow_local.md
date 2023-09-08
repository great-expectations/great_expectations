---
title: Get started with GX and filesystem data
description: A guide to installing GX and running through a basic workflow for the first time on data stored in CSV and similar files.
id: gx_workflow_local
tag: [tutorial]
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import PandasMultiple from '/docs/oss/proofs/end2end/_filesystem/_pandas.md'
import SparkMultiple from '/docs/oss/proofs/end2end/_filesystem/_spark.md'

This guide will walk you through installing Great Expectations (GX) and running through a basic workflow for the first time with data stored in CSV or similar files.

## Prerequisites

- An installation of Python, version 3.8 to 3.11
  - To download and install Python, see [Python's downloads page](https://www.python.org/downloads/)
- Internet access to install Python libraries
- Existing filesystem data to connect to

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::

## Install GX

To install GX run the following from your terminal command prompt:

```commandline
python -m pip install great_expectations
```

:::info Best practices

It is recommended that you install the GX library in a Python virtual environment, which you use specifically for your GX project.

:::

For more guidance on setting up your Python environment and installing GX, see [Install Great Expectations](/oss/docs/proofs/teaching/setup/install_gx.md).

## Start a GX project

In GX, you access a project through a Data Context object.

```python
import great_expectations as gx

context = gx.get_context()
```

:::info GX project types

The `get_context()` method will return the first that it finds of:
- A Great Expectations Cloud project
- A locally saved project

If neither of the above are found, `get_context()` will return:
- A new project created in-memory

In-memory GX projects don't persist beyond the current Python session unless you explicitly save them.

:::

For more information on creating, saving, and selecting projects in GX see: [Manage GX projects](/oss/proofs/end2end/_filesystem/_pandas.md).

## Connect to data

<Tabs
  queryString="datasource"
  groupId="data_type"
  defaultValue='pandas'
  values={[
  {label: 'Multiple files with Pandas', value:'pandas'},
  {label: 'Multiple files with Spark', value:'spark'},
  ]}>

<TabItem value="pandas">
  <PandasMultiple />
</TabItem>

<TabItem value="spark">
  <SparkMultiple />
</TabItem>

</Tabs>

For more guidance on connecting to and organizing file data in GX, see [Connect to filesystem data](/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data.md).

## Create Expectations

After you connect GX to your data, you will create a set of Expectations that describe the standards you expect your data to conform to.

```python
my_expectation_suite_name = "my_expectation_suite"
my_expectations = context.add_or_update_expectation_suite(my_expectation_suite_name)

my_expectations.expect_column_values_to_not_be_null(column="vendor_id")
my_expectations.expect_
my_expectations.expect_
my_expectations.expect_
```

For more guidance on how to create Expectations in GX, see [Create Expectations](/guides/expectations/expectations_lp.md).

## Validate data

Once you have defined a set of Expectations, you can validate your data against them.  A Checkpoint object will let you associate one or more selections of data with sets of Expectations to validate them against.

```python
checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    batch_request = data_asset.build_batch_request(),
    expectation_suite_name = my_expectation_suite_name
)

checkpoint.run()
```

For more guidance on creating a new Checkpoint and validating your data, see [Validate data with a Checkpoint](/guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).

## Review results

You can build and view documentation that GX generates from your Checkpoint results with:

```python
context.build_data_docs()
```

For more information on the GX generated documentation for your project, see [Customize and configure project documentation](/guides/setup/configuring_data_docs/host_and_share_data_docs.md).

## Next steps

You have now installed GX and run through a basic workflow for the first time.

Next, you can:
- [Further expand the scope of your project](/conceptual_guides/gx_overview.md) with additional data sources, sets of Expectations, and Checkpoints.
- [Customize the internal workings of your project](/guides/setup/setup_overview_lp.md) by configuring its documentation and metadata storage options.
- [Expand on the basic GX workflow](/docs/conceptual_guides/learn_lp.md) with data pipeline integration and iterative procedures.

## Quick reference

The terminology used in this guide consists of:
- **Data Context:** The Python object that contains a GX project.
- **Expectations:** The standards you expect your data to conform to.
- **Checkpoint:** The Python object that tells GX which Expectations to apply to which sets of data.
