---
title: My data is in a filesystem
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Introduction

On the filesystem, GE supports a variety of tabular data, including CSV, and Parquet.

## Where would you like computation to happen?

This determines which `Execution Engine` Great Expectations will use to perform computation on your data.
There are two choices:

- [pandas](#i-want-my-computation-to-occur-in-pandas) uses the `PandasExecutionEngine`
- [spark](#i-want-my-computation-to-occur-in-spark) uses the `SparkExecutionEngine`

### I want my computation to occur in Pandas

Here is an example of your configuration:

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L10-L22
```

:::info
Do we want this kind of learning-oriented material in this doc?
I don't have a strong opinion.
:::

***What does it contain?***
1. It has a PandasExecutionEngine
2. It also has a default `RuntimeDataConnector` named: `default_runtime_data_connector_name` (**TODO** discuss)
3. It also has a default batch_identifier : `default_identifier_name` (can be discussed)
4. A `RuntimeDataconnector` is usually enough to test whether your connection works, or if you are interactively validating a few Batches of Data.
5. There is also something called an ActiveDataConnector (Configured or Inferred) that can be used to automate the retrieval and validation of Batches while also enabling more sophisticated filtering and sorting.
Please look at (**TODO** insert link) for more information.

Now add the datasource by running:

:::info
Do we tell users how to actually add the datasource? Do we point them at the CLI?
:::

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L24
```


<Tabs
  defaultValue="csv_filesystem"
  values={[
    {label: 'CSV on a filesystem', value: 'csv_filesystem'},
    {label: 'TBD', value: 'tbd'},
  ]}>
  <TabItem value="csv_filesystem">

Here is an example of how to retrieve a batch of data using csvs on local filesystem.

```python file=../../../../integration/code/pandas/filesystem/csv_runtime_data_connector.py#L26-L34
```

  </TabItem>
  <TabItem value="tbd">Other things can go here!</TabItem>
</Tabs>


#### Test your configuration by ...

:::danger
This article is a stub.
:::


### I want my computation to occur in Spark

:::danger
This article is a stub.
:::
