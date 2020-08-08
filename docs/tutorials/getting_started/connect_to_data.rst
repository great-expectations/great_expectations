.. _tutorials__getting_started__connect_to_data:

Connect to data
===============

Once you have a DataContext initialized, you'll want to connect to data.  In Great Expectations, :ref:`Datasources` simplify connections, by managing configuration and providing a consistent, cross-platform API for referencing data.

Let's configure your first Datasource, by following the next steps in the CLI ``init`` flow. Note that these next steps match what happens when you run ``great_expectations datasource new``.
    
.. code-block:: bash
    
    Would you like to configure a Datasource? [Y/n]: 
    
    What data would you like Great Expectations to connect to?
        1. Files on a filesystem (for processing with Pandas or Spark)
        2. Relational database (SQL)
    : 2
    
    What are you processing your files with?
        1. TODO
        2. TODO
    : 5

    Give your new Datasource a short name.
     [my_postgres_db]:
    
    Great Expectations will now add a new Datasource 'my_postgres_db' to your deployment, by adding this entry to your great_expectations.yml:
    
      my_postgres_db:
        credentials: ${my_postgres_db}
        module_name: great_expectations.datasource
        data_asset_type:
          class_name: SqlAlchemyDataset
          module_name: great_expectations.dataset
        class_name: SqlAlchemyDatasource

    Would you like to proceed? [Y/n]: n

That's it, you just configured your first Datasource! Before continuing, let's stop and unpack what just happened.

Configuring Datasources
-----------------------

When you completed those last few steps, you told Great Expectations that

1. You want to create a new Datasource called ``my_postgres_db``.
2. The Datasource is of the type ``SqlAlchemyDatasource``
3. The credentials to connect to the database are stored in a variable called ``my_postgres_db`` which, by default, is located in the ``great_expectations/uncommitted/config_variables.py`` file.

Based on that information, the CLI added an entry into your ``great_expectations.yml`` file, under the ``datasources`` header.

In the future, you can modify or delete your configuration by editing your ``great_expectations.yml`` file directly. For instructions on how to configure various Datasources, check out :ref:`How-to guides for configuring Datasources <how_to_guides__configuring_datasources>`.

For now, let's continue to :ref:`tutorials__getting_started__create_your_first_expectations`.