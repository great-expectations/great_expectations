---
title: class DataContext
---
# DataContext
[See it on GitHub](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/data_context/data_context/data_context.py)

## Synopsis

A DataContext represents a Great Expectations project. It is the primary entry point for a Great Expectations
deployment, with configurations and methods for all supporting components.

The DataContext is configured via a yml file stored in a directory called great_expectations; this configuration
file as well as managed Expectation Suites should be stored in version control. There are other ways to create a
Data Context that may be better suited for your particular deployment e.g. ephemerally or backed by GE Cloud
(coming soon). Please refer to our documentation for more details.

You can Validate data or generate Expectations using Execution Engines including:

* SQL (multiple dialects supported)
* Spark
* Pandas

Your data can be stored in common locations including:

* databases / data warehouses
* files in s3, GCS, Azure, local storage
* dataframes (spark and pandas) loaded into memory

Please see our documentation for examples on how to set up Great Expectations, connect to your data,
create Expectations, and Validate data.

Other configuration options you can apply to a DataContext besides how to access data include things like where to
store Expectations, Profilers, Checkpoints, Metrics, Validation Results and Data Docs and how those Stores are
configured. Take a look at our documentation for more configuration options.

You can create or load a DataContext from disk via the following:
```
import great_expectations as ge
ge.get_context()
```


## Import statement

```python
from great_expectations.data_context.data_context.data_context import DataContext
```


## Public Methods (API documentation links)

- *[.create(...):](../methods/great_expectations-data_context-data_context-data_context-DataContext-create)* Build a new great_expectations directory and DataContext object in the provided project_root_dir.
- *[.test_yaml_config(...):](../methods/great_expectations-data_context-data_context-data_context-DataContext-test_yaml_config)* Convenience method for testing yaml configs

## Relevant documentation (links)

- [Data Context](../../terms/data_context)