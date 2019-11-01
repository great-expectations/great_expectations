.. _tutorial_create_expectations:

Create Expectations
==============================

Creating expectations is an opportunity to blend contextual knowledge from subject-matter experts and insights from
profiling and performing exploratory analysis on your dataset. This tutorial covers creating expectations for a data asset in the Jupyter notebook.

Video
------

If you prefer videos to written tutorials, `James <https://github.com/jcampbell>`_ (one of the original core contributors) walks you through this turorial in a `video on YouTube <https://greatexpectations.io/videos/getting_started/create_expectations>`_.


This tutorial assumes that you ran ``great_expectations init`` and went through the steps covered in the previous tutorial: :ref:`tutorial_init`.

Jupyter notebooks is the interface for creating expectations.

The ``great_expectations init`` command created ``great_expectations/notebooks/`` folder in your project. The folder contains example notebooks for pandas, Spark and SQL datasources. Choose the notebook directory that corresponds to your datasource. We will continue with the example we used in the previous section: CSV files containing National Provider Identifier (NPI) data that are processed with pandas. Since the datasource is pandas, we are going into this notebook: ``great_expectations/notebooks/pandas/create_expectations.ipynb``.


1. Get DataContext Object
-------------------------

A DataContext represents a Great Expectations project. It organizes datasources, notification settings, data documentation sites, and storage and access for expectation suites and validation results.
The DataContext is configured via a yml file stored in a directory called great_expectations;
the configuration file as well as managed expectation suites should be stored in version control.

Obtaining a DataContext object gets us access to these resources after the object reads its
configuration file.

::

    context = ge.data_context.DataContext()

To read more about DataContext, see: :ref:`data_context`



2. List Data Assets
-------------------

A Data Asset is data you describe with expectations.


It is useful to explain what can be a data asset in each type of datasource that Great Expectations supports:


.. content-tabs::

    .. tab-container:: tab0
        :title: pandas

        Pandas DataFrames are data assets in a Pandas datasource. In this example the pipeline processes NPI data that it reads from CSV files in ``npidata`` directory into Pandas DataFrames. This is the data we want to describe and specify with expectations. We think of this data asset as "data from the npidata directory that we read into Pandas DataFrames" and give it a name "NPI data".

    .. tab-container:: tab1
        :title: pyspark

        Spark DataFrames are data assets in a Spark datasource. In this example the pipeline processes NPI data that it reads from CSV files in ``npidata`` directory into Pandas DataFrames. This is the data we want to describe and specify with expectations. If the example read the data into Spark DataFrames, we would think of this data asset as "data from the npidata directory that we read into Spark DataFrames" and give it a name "NPI data".

    .. tab-container:: tab2
        :title: SQLAlchemy

        Tables, views and query results can be data assets in a SQLAlchemy datasource.

        * If the pipeline in this example used SQL to process NPI data that resided in ``npidata`` table (or view) in a database (as opposed to using Pandas to process NPI data that resides in CSV files in ``npidata`` folder), that table would be a data asset named ``npidata``.
        * If the NPI data did not reside in one table ``npidata`` and, instead, the example pipeline ran an SQL query that fetched the data (probably from multiple tables), we would consider the result set of that query to be the data asset. The name of this data asset would be up to us (e.g., "npidata" or "npidata_query").


Great Expectations' ``jupyter_ux`` module has a convenience method that lists all data assets and expectation suites in a Data Context:

.. code-block:: python

    great_expectations.jupyter_ux.list_available_data_asset_names(context)

Here is the output of this method when executed in our example project:

.. image:: ../images/list_data_assets.png
    :width: 600px

``npidata`` is the short name of the data asset. Full names of data assets in a DataContext consist of three parts that look like that: ``data__dir/default/npidata``. You don't need to know (yet) how the namespace is managed and the exact meaning of each part. The :ref:`data_context` article describes this in detail.


3. Pick a data asset and set the expectation suite name
-------------------------------------------------------

.. code-block:: python

    data_asset_name = "npidata"
    normalized_data_asset_name = context.normalize_data_asset_name(data_asset_name)
    normalized_data_asset_name


.. code-block:: python

    expectation_suite_name = "warning"

4. Create a new empty expectation suite
---------------------------------------

Expectations for a data asset are organized into expectation suites. Usually, we validate data against expectation suites, not single expectations. We recommend 'warning' or 'default' as the name
for a first expectation suite associated with a data asset.

.. code-block:: python

    context.create_expectation_suite(data_asset_name=data_asset_name,
                                     expectation_suite_name=expectation_suite_name)


If an expectation suite with this name already exists for this data_asset, you will get an error. If you would like to overwrite this expectation suite, set ``overwrite_existing=True``.


5. Load a batch of data you want to use to create Expectations
--------------------------------------------------------------

Expectations describe and specify data assets. What is validated at validation time are batches of data. A batch is a sample from a data asset, sliced according to a particular rule. If a database table is a data asset, then the data in that table snapshotted at a particular time is a batch.

To create expectations about a data asset you will load a sample batch of data from that data asset into a Great Expectations object of class :class:`Dataset <great_expectations.dataset.dataset.Dataset>` and then use the expectation methods (one method per expectation type) this class provides to add expectations, while immediately checking if the sample batch conforms to these expectations (this will become clear in the next section of this tutorial).

DataContext's ``get_batch`` method is used to load a batch of a data asset:

.. code-block:: python

    batch = context.get_batch(normalized_data_asset_name,
                              expectation_suite_name,
                              batch_kwargs)


Calling this method asks the Context to get a batch of data from the data asset ``normalized_data_asset_name`` and attach the expectation suite ``expectation_suite_name`` to it. ``batch_kwargs`` argument specifies which batch of the data asset should be loaded.

If you have no preference as to which batch should be sampled from the data asset, use the ``yield_batch_kwargs`` method on the data context to get a batch:

.. code-block:: python

    batch_kwargs = context.yield_batch_kwargs(data_asset_name)

This is most likely sufficient for the purpose of this tutorial.

.. toggle-header::
    :header: However, if you want to use a specific batch, **click here to learn how to specify the right batch_kwargs**

        .. content-tabs::

            .. tab-container:: tab0
                :title: pandas

                If the data asset is a collection of CSV files in a folder that are processed with Pandas, then a batch is one of these files. Here is how to construct batch_kwargs that specify a particular file to load:

                .. code-block:: python

                    batch_kwargs = {'path': "PATH OF THE FILE YOU WANT TO LOAD"}

                To instruct ``get_batch`` to read CSV files with specific options (e.g., not to interpret the first line as the
                header or to use a specific separator), add them to the the ``batch_kwargs``.

                See the complete list of options for `Pandas read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`__.

                ``batch_kwargs`` might look like the following:

                .. code-block:: json

                    {
                        "path": "/data/npidata/npidata_pfile_20190902-20190908.csv",
                        "partition_id": "npidata_pfile_20190902-20190908",
                        "sep": null,
                        "engine": "python"
                    }

            .. tab-container:: tab1
                :title: pyspark

                If the data asset is a collection of CSV files in a folder that are processed with pyspark, then a batch is one of these files. Here is how to construct batch_kwargs that specify a particular file to load:

                .. code-block:: python

                    batch_kwargs = {'path': "PATH OF THE FILE YOU WANT TO LOAD"}

                To instruct ``get_batch`` to read CSV files with specific options (e.g., not to interpret the first line as the
                header or to use a specific separator), add them to the the ``batch_kwargs``.

                See the complete list of options for `Spark DataFrameReader <https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader>`__

            .. tab-container:: tab2
                :title: SQLAlchemy

                Here are three examples of constructing ``batch_kwargs`` that specify which batch of data should be loaded from a data asset:

                If you would like to validate an entire table (or a view) in your database's default schema:

                .. code-block:: python

                    batch_kwargs = {'table': "YOUR TABLE NAME"}

                If you would like to validate an entire table or view from a non-default schema in your database:

                .. code-block:: python

                    batch_kwargs = {'table': "YOUR TABLE NAME", "schema": "YOUR SCHEMA"}

                If you would like to validate using a query to construct a temporary table:

                .. code-block:: python

                    batch_kwargs = {'query': 'SELECT YOUR_ROWS FROM YOUR_TABLE'}


        A batch is a sample from a data asset, sliced according to a particular rule. Generators are classes that implement these rules in Great Expectations. The examples of ``batch_kwargs`` above are the inputs of the default Generators that come with Great Expectations. You can read about the default Generators' behavior and how to implement additional generators in this article: :ref:`batch_generator`.


|
The previous call loaded one of the batches of the ``data__dir/default/npidata`` data asset (one of the files).


6. Author Expectations
-----------------------

Now that we have one of the data batches loaded, we can call ``expect`` methods on the data asset in order to check
whether this expectation is true for this batch of data.

For example, to check if we can expect values in column "NPI" to never be empty, call:
``df.expect_column_values_to_not_be_null('NPI')``

Some expectations can be created from your domain expertise; for example we might expect that most entries in the NPI
database use the title "Dr." instead of "Ms.", or we might expect that every row should use a unique value in the 'NPI'
column.

Here is how we can add an expectation that expresses that knowledge:

.. image:: ../images/expect_column_values_to_be_unique_success.png

Other expectations can be created by examining the data in the batch. For example, we want to protect our pipeline
against improper values in the "Provider Other Organization Name Type Code" column. We don't know exactly what the
"improper" values are, but we can try some values and check if the data in the batch meets this expectation:

.. image:: ../images/expect_column_values_to_be_in_set_failure.png

Validating the expectation against the batch resulted in failure - there are some values in the column that do not meet
the expectation. The "partial_unexpected_list" key in the result dictionary contains examples of non-conforming values.
Examining these examples shows that some titles are not in our expected set. We adjust the ``value_set`` and rerun
the expectation method:

.. image:: ../images/expect_column_values_to_be_in_set_success.png

This time validation was successful - all values in the column meet our expectation.

Although we called ``expect_column_values_to_be_in_set`` twice (with different argument values), only one
expectation of type ``expect_column_values_to_be_in_set`` will be created for the column - the latest call
overrides all the earlier ones. By default, only expectations that were true on their last run are saved.

How do I know which types of expectations I can add?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* *Tab-complete* the partially typed ``expect_`` method name to see available expectations.
* In Jupyter, we can also use *shift-tab* to see the docstring for each expectation, including the parameters it
  takes and to get more information about the expectation.
* Visit the `glossary of expectations <https://docs.greatexpectations.io/en/latest/glossary.html>`__ for a complete
  list of expectations that are currently part of the great expectations vocabulary.

.. image:: ../images/glossary_of_expectations_preview.png
    :width: 400px


7. Review and save your Expectations
------------------------------------

.. image:: ../images/get_expectation_suite_output.png

.. code-block:: python

    df.save_expectation_suite()

Because this data asset is connected to the DataContext, GE determines the location to save the expectation suite:

When we call ``get_expectation_suite``, we might see this warning in the output:

.. image:: ../images/failing_expectations_warning.png

When we save an expectation suite, by default, GE will drop any expectation that was not successful on its last run.

Sometimes we want to save an expectation even though it did not validate successfully on the current batch (e.g., we
have a reason to believe that our expectation is correct and the current batch has bad entries). In this case we pass
an additional argument to ``save_expectation_suite`` method:

.. code-block:: python

    df.save_expectation_suite(discard_failed_expectations=False)


8. View the Expectations in Data Docs
-------------------------------------

.. code-block:: python


    context.build_data_docs()
    context.open_data_docs()
