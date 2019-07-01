"""A DataContext represents a Great Expectations project. It organizes storage and access for
expectation suites, datasources, notification settings, and data fixtures.

The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
as well as managed expectation suites should be stored in version control.

DataContexts use data sources you're already familiar with. Generators help introspect data stores and data execution
frameworks (such as airflow, nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
enables fetching, validation, profiling, and documentation of  your data in a way that’s meaningful within your
existing
infrastructure and work environment.

DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
normalized *data_asset_name*, consisting of *datasource/generator/generator_asset*.

- The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets \
connected to a compute environment and ready for validation.

- The Generator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
particular slices of data.

- The generator_asset is a specific name -- often a table name or other name familiar to users -- that \
generators can slice into batches.

An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
in many projects it is useful to have different expectations evaluate in different contexts--profiling 
vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace 
option for selecting which expectations a DataContext returns.

In many simple projects, the datasource or generator name may be omitted and the DataContext will infer
the correct name when there is no ambiguity.

Similarly, if no expectation suite name is provided, the DataContext will assume the name "default"
"""


from .data_context import DataContext
