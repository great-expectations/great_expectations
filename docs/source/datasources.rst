.. _datasources:

Datasources
===========

.. automodule:: great_expectations.datasource
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: DataContext

    .. autoclass:: great_expectations.datasource.Datasource
        :members:
        :undoc-members:
        :show-inheritance:








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