---
title: My data is in a filesystem
---

# Introduction

On the filesystem, GE supports a variety of tabular data, including CSV, and Parquet.

You will first be asked to answer a few questions like :

## Where would you like computation to happen?

This question is for determining the Execution Engine, which is what GE uses to handle. The IN the case.

We have PandasExecutionEngine, and SparkExecutionEngine to choose from.


### I want my computation to occur in `Pandas`

Great here is an example of your configuration:

```python file=../../../../integration/code/path_filesystem_runtime_data_connector.py#L5-L17
```
***What does it contain?***
1. It has a PandasExecutionEngine
2. It also has a default RuntimeDataConnector
  - name: default_runtime_data_connector_name (can be discussed)

3. It also has a default batch_identifier : default_identifier_name (can be discussed)

4. A RuntimeDataconnector is usually enough to test whether your connection works, or if you are interactively validating a few Batches of Data.

5. There is also something called an ActiveDataConnector (Configured or Inferred) that can be used to automate the retrieval and validation of Batches while also enabling more sophisticated filtering and sorting. Please look at (insert link) for more information.


```python file=../../../../integration/code/path_filesystem_runtime_data_connector.py#L20
```

#### Test your configuration in




### I want my computation to occur in `Spark`

:::tip
This article is a stub.
:::
