---
title: How to connect to your data on a filesystem using pandas
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on a filesystem using pandas. This enables you to work with your data in Great Expectations.

:::note Prerequisites: This how-to guide assumes you have already:
- Completed the [Getting Started Tutorial](../../../tutorials/getting-started/intro.md)
- Have a working installation of Great Expectations
- Have access to data on a filesystem
:::

## Steps

### 1. Add the datasource to your project

Using this example configuration:

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L8-L22
```

Add the datasource by using the CLI or python APIs:

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

**TODO** flesh out this section - link to an article about how to add a datasource with the CLI?

In the notebook...

  </TabItem>
  <TabItem value="python">

Import some libraries

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L1-L4
```

Load a DataContext

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L7
```

Save the Datasource configuration you created above into your Data Context

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L23
```

</TabItem>
</Tabs>

### 3. Write a `BatchRequest`

In a `RuntimeBatchRequest` the `data_asset_name` can be any unique name to identify this batch of data. Please update the `data_asset_name` to something meaningful to you.

<Tabs
  defaultValue="csv"
  values={[
    {label: 'CSV', value: 'csv'},
    {label: 'CSV with custom delimiters', value: 'csv_delimiters'},
    {label: 'CSV without a header', value: 'csv_headerless'},
  ]}>
  <TabItem value="csv">

Add the path to your csv on a filesystem to the `path` key under `runtime_parameters` and run the following code:

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L24-L31
```

  </TabItem>
  <TabItem value="csv_delimiters">

  **TODO** do this

  </TabItem>
  <TabItem value="csv_headerless">

  **TODO** do this

  </TabItem>
</Tabs>

### 4. Test your datasource configuration by getting a `Batch`

Load a `Batch` of your data to test if your `Datasource` configuration works by running the following code:

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L39
```

Congratulations! If no errors are shown here, you've just connected to your data on a filesystem using pandas!

:::tip Next steps
After connecting to data, usually people validate data!
**TODO** insert link or have this be an include.

Right now a Batch is just Data + Metadata that is recognized by GE. If you want to actually Validate your data with Expectations, you will need a Batch + ExpectationSuite.
:::

## Additional Notes

**TODO**
Consider removing learning-focused material.

:::note What does this configuration contain?

- It has a `PandasExecutionEngine` which provides the computing resources that will be used to perform validation on your data.
- It also has a default `RuntimeDataConnector` named: `default_runtime_data_connector_name` (**TODO** discuss) which will loads your data into a `Batch`.
- It also has a default batch_identifier : `default_identifier_name` (**TODO** discuss) used to uniquely identify Batches
:::

:::note A word about DataConnectors
A `RuntimeDataconnector` is usually enough to test whether your connection works (as we are doing now), or in cases where you are using Great Expectations to interactively validate a few Batches of Data.

Great Expectations also has something called an `ActiveDataConnector` (Configured or Inferred) that can be used to automate the retrieval and validation of Batches while also enabling more sophisticated filtering and sorting. Please look at (**TODO** insert link) for more information.
:::

### Full script

Here is the full script from which snippets were taken.

```python file=../../../../../integration/code/connecting_to_your_data/filesystem/pandas.py#L1-L40
```
