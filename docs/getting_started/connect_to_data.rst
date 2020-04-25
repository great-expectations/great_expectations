.. _getting_started__connect_to_data:

Connect to data
===============================================

Once you have a DataContext, you'll want to connect to data. To make this work, let's configure our first Datasource, using the init flow:

.. code-block:: bash

    What data would you like Great Expectations to connect to?    
        1. Files on a filesystem (for processing with Pandas or Spark)
        2. Relational database (SQL)
    : 1

    What are you processing your files with?
        1. Pandas
        2. PySpark
    : 1

    Enter the path (relative or absolute) of a data file
    : data/notable_works_by_charles_dickens/notable_works_by_charles_dickens.csv

    #FIXME: Show the generator output.

    Generator configured:

    notable_works_by_charles_dickens:
      class_name: PandasDatasource
      data_asset_type:
        class_name: PandasDataset

That's it! You just configured your first Datasource!

Before continuing, let's stop and unpack what just happened, and why.

Why Data Sources?
-----------------------------------------------------

*Validation* is the core operation in Great Expectations: "validate X data against Y Expectations."

Although the concept of data validation is simple, carrying it out can require complex engineering. This is because your Expectations and data might be stored in different places, and the computational resources for validation might live somewhere else entirely. The engineering cost of building the necessary connectors for validation has been one of the major things preventing blockers for data teams that want to test their data.

Datasources solve this problem, by conceptually separating *what* you want to validate from *how* you to validate it.  Datasources give you full control over the process of bringing data and Expectations together, then abstract away that underlying complexity when you validate X data against Y Expectations.

Configuring data sources
-----------------------------------------------------

#FIXME: More work needed here

The declaration for a Datasource includes at least three components, two required and one optional:

* ``class_name`` (required): specifies the class that will be used to get Batches of data. Most of these classes are thin wrappers to other data tools. For example, `PandasDatasource` uses pandas to read a .csv or other file. You can see the full list :ref:`here <...>`.

* ``data_asset_type`` (required): specifies the execution environment in which Expectations will be executed. Great Expectations currently supports three execution environments: pandas, sqlalchemy, and pyspark. We will likely extend the library to support others in the future.

#FIXME: Do we even need data_asset_type? Shouldn't it always be the same thing as ``class_name`` (e.g. pandas and pandas, sql and sql)? Do we have any examples where this is NOT true?

Configuring a DataSource with just `class_name` and `data_asset_type`

* `generators` (optional): allow you to streamline the production of `BatchKwargs`. For now, think of them as an advanced feature. We’ll come back to them at the end of the tutorial.

Once configured, Datasources produce Batches, which can then be validated using Expectations. We'll dig into Batches and Expectations later in the tutorial.

