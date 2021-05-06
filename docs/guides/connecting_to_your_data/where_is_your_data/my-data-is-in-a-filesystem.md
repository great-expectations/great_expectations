---
title: My data is in a filesystem
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on a filesystem. This enables you to work with your data in Great Expectations.

:::note Prerequisites: This how-to guide assumes you have already:
- Completed the [Getting Started Tutorial](../../../tutorials/getting-started/intro.md)
- Have a working installation of Great Expectations
- Have access to data on a filesystem
:::

## Steps

### 1. Choose an `ExecutionEngine`

Which compute backend would you like to perform your data validations?

There are two choices:

- [pandas](#i-want-my-computation-to-occur-in-pandas) (uses the `PandasExecutionEngine`)
- [spark](#i-want-my-computation-to-occur-in-spark) (uses the `SparkExecutionEngine`)

#### I want my computation to occur in Pandas

Here is an example of your configuration:

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L8-L22
```

#### I want my computation to occur in Spark

:::danger TODO this is a stub
:::

### 2. Add the datasource to your project

Now add the datasource by running:

**TODO** Do we tell users how to actually add the datasource? Do we point them at the CLI? Do we provide both options in tabs or separate files that are included?

<Tabs
  defaultValue="python"
  values={[
    {label: 'CLI', value: 'cli'},
    {label: 'python', value: 'python'},
  ]}>
  <TabItem value="cli">

Run this command:

```console
great_expectations --v3-api datasource new
```

**TODO** flesh out this section - link to an article about how to add a datasource with the CLI.

In the notebook...

  </TabItem>
  <TabItem value="python">

Import some libraries

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L1-L4
```

Load a DataContext

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L7
```

Save the Datasource configuration you created above to your Data Context

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L23
```
**TODO** flesh out this section

</TabItem>
</Tabs>

### 3. Write a BatchRequest

:::note
In a `RuntimeBatchRequest` the `data_asset_name` can be any unique name to identify this batch of data.
:::

<Tabs
  defaultValue="csv_filesystem"
  values={[
    {label: 'CSV on a filesystem', value: 'csv_filesystem'},
    {label: 'CSV on S3', value: 'csv_s3'},
    {label: 'CSV on GCS', value: 'csv_gcs'},
    {label: 'CSV on Azure', value: 'csv_azure'},
  ]}>
  <TabItem value="csv_filesystem">

Add the path to your csv on a filesystem to the `path` key under `runtime_parameters` and run the following code:

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L24-L31
```

  </TabItem>
  <TabItem value="csv_s3">

  **TODO** S3

  </TabItem>
  <TabItem value="csv_gcs">

  **TODO** GCS

  </TabItem>
  <TabItem value="csv_azure">

  **TODO** Azure

  </TabItem>
</Tabs>

### 4. Test your datasource configuration

Load a `Batch` of your data to test if your `Datasource` configuration works by running the following code:

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L39
```
**TODO** What does success look like here? No stacktraces?

## Additional Notes

**TODO** Do we want to provide the full script? Pros: copy-n-paste glory. Cons: some scripts have some necessary hacky bits (for example connection strings that work in tests and also look friendly for users).

#### Full script

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py
```

**TODO** maybe the didactic material goes here if it is required...

Right now a Batch is just Data + Metadata that is recognized by GE. If you want to actually Validate your data with Expectations, you will need a Batch + ExpectationSuite. Please see the next section.

**TODO**
Do we want this kind of learning-oriented material in this doc?
I don't have a strong opinion.

:::note What does this configuration contain?

- It has a `PandasExecutionEngine` which provides the computing resources that will be used to perform validation on your data.
- It also has a default `RuntimeDataConnector` named: `default_runtime_data_connector_name` (**TODO** discuss) which will loads your data into a `Batch`.
- It also has a default batch_identifier : `default_identifier_name` (**TODO** discuss) used to uniquely identify Batches
:::

:::note A word about DataConnectors
A `RuntimeDataconnector` is usually enough to test whether your connection works (as we are doing now), or in cases where you are using Great Expectations to interactively validate a few Batches of Data.

Great Expectations also has something called an `ActiveDataConnector` (Configured or Inferred) that can be used to automate the retrieval and validation of Batches while also enabling more sophisticated filtering and sorting. Please look at (**TODO** insert link) for more information.
:::
