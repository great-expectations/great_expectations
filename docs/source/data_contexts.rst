.. _data_contexts:


================================================================================
Data Contexts
================================================================================

A DataContext represents a Great Expectations project. It captures essential information such as
expectation suites, datasources, notification settings, and data fixtures.

The DataContext is configured via a yml file that should be stored in a file called
great_expectations/great_expectations.yml under the context_root_dir passed during initialization.

DataContexts use data sources you're already familiar with. Convenience libraries called generators
help introspect data stores and data execution frameworks airflow, dbt, dagster, prefect.io) to produce
batches of data ready for analysis. This lets you fetch, validate, profile, and document your data in
a way that’s meaningful within your existing infrastructure and work environment.

DataContexts use a datasource-based namespace, where each accessible type of data has a four-part
normaled data_asset_name, consisting of:

**datasource / generator / generator_asset / expectation_suite**

  - The datasource actually connects to a source of materialized data and returns Great Expectations
    DataAssets connected to a compute environment and ready for validation.

  - The Generator knows how to introspect datasources and produce identifying "batch_kwargs" that define
    particular slices of data.

  - The generator_asset is a specific name -- often a table name or other name familiar to users -- that
    generators can slice into batches.

  - The expectation_suite is a collection of expectations ready to be applied to a batch of data. Since
    in many projects it is useful to have different expectations evaluate in different contexts--profiling 
    vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace 
    option for selecting which expectations a DataContext returns.

In many simple projects, the datasource, generator and/or expectation_suite can be ommitted and a
sensible default (usually "default") will be used.

To get a data context, simply call `get_data_context()` on the ge object:




There are currently four types of data contexts:
  - :ref:`PandasCSVDataContext`: The PandasCSVDataContext ('PandasCSV') exposes a local directory containing files as datasets.
  - :ref:`SqlAlchemyDataContext`: The SqlAlchemyDataContext ('SqlAlchemy') exposes tables from a SQL-compliant database as datasets.
  - :ref:`SparkCSVDataContext`: The SparkCSVDataContext ('SparkCSV') exposes csv files accessible from a SparkSQL context.
  - :ref:`SparkParquetDataContext`: The SparkParquetDataContext ('SparkParquet') exposes parquet files accessible from a SparkSQL context.
  - :ref:`DatabricksTableContext`: The DatabricksTableContext ('DatabricksTable') exposes tables from a databricks notebook.

All data contexts expose the following methods:
  - list_datasets(): lists datasets available in current context
  - get_dataset(dataset_name): returns a dataset with the matching name (e.g. filename or tablename)

.. _PandasCSVDataContext:

`PandasCSVDataContext`
----------------------

The `options` paramater for a PandasCSVDataContext is simply the glob pattern matching the files to be available.


.. _SqlAlchemyDataContext:

`SqlAlchemyDataContext`
-----------------------

The `options` parameter for a SqlAlchemyDataContext is the sqlalchemy connection string to connect to the database.


.. _SparkCSVDataContext:

`SparkCSVDataContext`
---------------------

The `options` parameter for a SparkCSVDataContext is a directory from which to read a CSV file, and options to pass to the reader.


.. _SparkParquetDataContext:

`SparkParquetDataContext`
-------------------------

The `options` parameter for a SparkParquetDataContext is a directory from which to read a Parquet file, and options to pass to the reader.


.. _DatabricksTableContext:

`DatabricksTableContext`
---------------------

The `options` parameter for a _DatabricksTableContext is a dataase from which to expose tables; get_dataset optionally also accepts
a date partition.