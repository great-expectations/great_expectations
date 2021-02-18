.. _tutorials__getting_started__connect_to_data:

Connect to data
====================

Once you have a DataContext, you'll want to connect to data.  In Great Expectations, :ref:`Datasources <reference__core_concepts__datasources>` simplify connections, by managing configuration and providing a consistent, cross-platform API for referencing data.

Let's configure your first Datasource: a connection to the data directory we've provided in the repo. This could also be a database connection, but for now we're just using a simple file store:
    
.. code-block:: bash
    
    Would you like to configure a Datasource? [Y/n]: <press enter>

    What data would you like Great Expectations to connect to?
        1. Files on a filesystem (for processing with Pandas or Spark)
        2. Relational database (SQL)
    : 1

    What are you processing your files with?
        1. Pandas
        2. PySpark
    : 1

    Enter the path (relative or absolute) of the root directory where the data files are stored.
    : data

    Give your new Datasource a short name.
     [data__dir]: <press enter>

    ... <some more output here> ...

    Would you like to proceed? [Y/n]:
    A new datasource 'data__dir' was added to your project.

    Would you like to profile new Expectations for a single data asset
    within your new Datasource? [Y/n]: n

That's it! **You just configured your first Datasource!**

Make sure to choose ``n`` at this prompt to exit the ``init`` flow for now. Normally, the ``init`` flow takes you through another step to create sample Expectations, but we want to jump straight to creating an Expectation Suite using the ``scaffold`` method next.

**Before continuing, let's stop and unpack what just happened.**


Configuring Datasources
---------------------------

When you completed those last few steps in ``great_expectations init``, you told Great Expectations that:

1. You want to create a new Datasource called ``data__dir``.
2. You want to use Pandas to read the data from CSV.

Based on that information, the CLI added the following entry into your ``great_expectations.yml`` file, under the ``datasources`` header:

.. code-block:: yaml

      data__dir:
        data_asset_type:
          class_name: PandasDataset
          module_name: great_expectations.dataset
        batch_kwargs_generators:
          subdir_reader:
            class_name: SubdirReaderBatchKwargsGenerator
            base_directory: ../data
        class_name: PandasDatasource
        module_name: great_expectations.datasource

This datasource does not require any credentials. However, if you were to connect to a database that requires connection credentials, those would be stored in ``great_expectations/uncommitted/config_variables.yml``.

In the future, you can modify or delete your configuration by editing your ``great_expectations.yml`` and ``config_variables.yml`` files directly.

**For now, let's move on to creating your first Expectations.**
