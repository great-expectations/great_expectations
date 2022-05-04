---
title: class DataContext
---
# DataContext
[See it on GitHub](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/data_context/data_context/data_context.py)

## Synopsis

A DataContext represents a Great Expectations project. It organizes storage and access for
expectation suites, datasources, notification settings, and data fixtures.

The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
as well as managed expectation suites should be stored in version control.

Use the `create` classmethod to create a new empty config, or instantiate the DataContext
by passing the path to an existing data context root directory.

DataContexts use data sources you're already familiar with. BatchKwargGenerators help introspect data stores and data execution
frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
enables fetching, validation, profiling, and documentation of your data in a way that is meaningful within your
existing infrastructure and work environment.

DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
normalized *data_asset_name*, consisting of *datasource/generator/data_asset_name*.

- The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets connected to a compute environment and ready for validation.

- The BatchKwargGenerator knows how to introspect datasources and produce identifying "batch_kwargs" that define particular slices of data.

- The data_asset_name is a specific name -- often a table name or other name familiar to users -- that batch kwargs generators can slice into batches.

An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
in many projects it is useful to have different expectations evaluate in different contexts--profiling
vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
option for selecting which expectations a DataContext returns.

In many simple projects, the datasource or batch kwargs generator name may be omitted and the DataContext will infer
the correct name when there is no ambiguity.

Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".


## Import statement

```python
from great_expectations.data_context.data_context.data_context import DataContext
```


## Public Methods (API documentation links)

*[.create(...):](/docs/api_docs/methods/great_expectations-data_context-data_context-data_context-DataContext-create)* Build a new great_expectations directory and DataContext object in the provided project_root_dir.
*[.test_yaml_config(...):](/docs/api_docs/methods/great_expectations-data_context-data_context-data_context-DataContext-test_yaml_config)* Convenience method for testing yaml configs

## Relevant documentation (links)

- [Data Context](/docs/terms/data_context)