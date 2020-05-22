.. _getting_started__connect_to_data:

Connect to data
===============

Once you have a DataContext, you'll want to connect to data.  In Great Expectations, :ref:`Datasources` simplify connections, by managing configuration and providing a consistent, cross-platform API for referencing data.

Let's configure your first Datasource, by following the next steps in the CLI init flow:

.. code-block:: bash

    Would you like to configure a Datasource? [Y/n]: 

    What data would you like Great Expectations to connect to?    
        1. Files on a filesystem (for processing with Pandas or Spark)
        2. Relational database (SQL)
    : 1

    What are you processing your files with?
        1. Pandas
        2. PySpark
    : 1

    Enter the path (relative or absolute) of the root directory where the data files are stored.
    : /Users/eugenemandel/project_data/womens-shoes-prices

    Give your new Datasource a short name.
      [womens-shoes-prices__dir]: 

    Great Expectations will now add a new Datasource 'womens-shoes-prices__dir' to your deployment,
    by adding this entry to your great_expectations.yml:

      womens-shoes-prices__dir:
        data_asset_type:
          class_name: PandasDataset
          module_name: great_expectations.dataset
        batch_kwargs_generators:
          subdir_reader:
            class_name: SubdirReaderBatchKwargsGenerator
            base_directory: /Users/eugenemandel/project_data/womens-shoes-prices

    OK to proceed? [Y/n]: 


That's it! You just configured your first Datasource!

Before continuing, let's stop and unpack what just happened, and why.

Why Datasources?
----------------

*Validation* is the core operation in Great Expectations: "Validate X data against Y Expectations."

Although the concept of data validation is simple, carrying it out can require complex engineering. This is because your Expectations and data might be stored in different places, and the computational resources for validation might live somewhere else entirely. The engineering cost of building the necessary connectors for validation has been one of the major things preventing data teams from testing their data.

Datasources solve this problem, by conceptually separating *what* you want to validate from *how* you want to validate it. Datasources give you full control over the process of bringing data and Expectations together, then abstract away that underlying complexity when you validate X data against Y Expectations.

.. figure:: ../../images/datasource-conceptual-diagram.png
    :width: 400px
    :class: with-shadow float-right

We call the layer that handles the actual computation an :ref:`Execution Engine <reference__core_concepts__validation__execution_engines>`. Currently, Great Expectations supports three Execution Engines: pandas, sqlalchemy, and pyspark. We plan to extend the library to support others in the future.

The layer that handles connecting to data is called a :ref:`Batch Kwargs Generator <reference__core_concepts__batch_kwargs_generators>`. Not all Batch Kwarg Generators can be used with all Execution Engines. It's also possible to configure a Datasource without a Batch Kwargs Generator if you want to pass data in directly. However, Batch Kwarg Generators are required to get the most out of Great Expectations' :ref:`Data Docs` and :ref:`Profilers`.

You can read more about the inner workings of Datasources, Execution Engines, and Batch Kwargs Generators :ref:`here <Validation>`.

.. attention::

    We plan to upgrade this configuration API with better names and more conceptual clarity prior to Great Expectations' 1.0 release. If at all possible, we will make those changes in a non-breaking way. If you have ideas, concerns or questions about this planned improvement, please join the public discussion `here <https://discuss.greatexpectations.io/t/conceptual-mismatches-in-datasource-internals/134>`__.


Configuring Datasources
-----------------------

When you completed those last few steps in ``great_expectations init``, you told Great Expectations that

1. You want to create a new Datasource called ``womens-shoes-prices__dir``.
2. You want to use Pandas as your :ref:`Execution Engine <Execution Engines>`, hence ``data_asset_type.class_name = PandasDataset``.
3. You want to create a BatchKwarg Generator called ``subdir_reader`` using the class ``SubdirReaderBatchKwargsGenerator``.
4. This particular Generator connects to data in files within a local directory, specified here as ``/Users/eugenemandel/project_data/womens-shoes-prices``.

Based on that information, the CLI added the following entry into your ``great_expectations.yml`` file, under the ``datasources`` header:

.. code-block:: yaml

    womens-shoes-prices__dir:
      data_asset_type:
        class_name: PandasDataset
        module_name: great_expectations.dataset
      batch_kwargs_generators:
        subdir_reader:
          class_name: SubdirReaderBatchKwargsGenerator
          base_directory: /Users/eugenemandel/project_data/womens-shoes-prices

In the future, you can modify or delete your configuration by editing your ``great_expectations.yml`` file directly. For instructions on how to configure various Datasources, check out :ref:`How-to guides for configuring Datasources <how_to_guides__configuring_datasources>`.

For now, let's continue to :ref:`getting_started__create_your_first_expectations`.