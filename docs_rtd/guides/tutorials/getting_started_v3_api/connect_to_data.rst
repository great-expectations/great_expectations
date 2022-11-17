.. _tutorials__getting_started_v3_api__connect_to_data:

Connect to data
====================

Once you have a Data Context, you'll want to connect to data.  In Great Expectations, :ref:`Datasources <reference__core_concepts__datasources>` simplify connections, by managing configuration and providing a consistent, cross-platform API for referencing data.

Let's configure your first Datasource: a connection to the data directory we've provided in the repo. This could also be a database connection, but for now we're just using a simple file store.

Start by running the following command:

.. code-block:: bash

    great_expectations --v3-api datasource new
    
.. code-block:: bash

    What data would you like Great Expectations to connect to?
        1. Files on a filesystem (for processing with Pandas or Spark)
        2. Relational database (SQL)
    : 1

    What are you processing your files with?
        1. Pandas
        2. PySpark
    : 1

    Enter the path of the root directory where the data files are stored. If files are on local disk enter a path relative to your current working directory or an absolute path.
    : data

This will now **open up a new Jupyter notebook** to complete the Datasource configuration.

The ``datasource new`` notebook
--------------------------------

The Jupyter notebook contains some boilerplate code to configure your new Datasource. You can run the entire notebook as-is,
but we recommend changing at least the Datasource name to something more specific.

Edit the second code cell as follows:

.. code-block:: python

    datasource_name = "data__dir"

Then **execute all cells in the notebook** in order to save the new Datasource. If successful, the last cell
will print a list of all Datasources, including the one you just created.

**Before continuing, let's stop and unpack what just happened.**


Configuring Datasources
---------------------------

When you completed those last few steps, you told Great Expectations that:

1. You want to create a new Datasource called ``data__dir``.
2. You want to use Pandas to read the data from CSV.

Based on that information, the CLI added the following entry into your ``great_expectations.yml`` file, under the ``datasources`` header:

.. code-block:: yaml

    name: my_datasource
    class_name: Datasource
    execution_engine:
      class_name: PandasExecutionEngine
    data_connectors:
      my_datasource_example_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        datasource_name: my_datasource
        base_directory: ../data
        default_regex:
          group_names:
            - data_asset_name
          pattern: (.*)

This datasource does not require any credentials. However, if you were to connect to a database that requires connection credentials,
those would be stored in ``great_expectations/uncommitted/config_variables.yml``.

In the future, you can modify or delete your configuration by editing your ``great_expectations.yml`` and ``config_variables.yml`` files directly.

**For now, let's move on to creating your first Expectations.**
