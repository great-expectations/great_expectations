.. _tutorial_create_expectations:

How to create Expectations
==========================

.. warning:: This doc has been partially migrated from old documentation. It's potentially helpful, but may be incomplete, incorrect, or confusing.

This tutorial covers the workflow of creating and editing expectations.

The tutorial assumes that you have created a new Data Context (project), as covered here: :ref:`tutorials__getting_started`.

Creating expectations is an opportunity to blend contextual knowledge from subject-matter experts and insights from
profiling and performing exploratory analysis on your dataset.

Once the initial setup of Great Expectations is complete, the workflow looks like a loop over the following steps:

1. Data team members capture and document their shared understanding of their data as expectations.
2. As new data arrives in the pipeline, Great Expectations evaluates it against these expectations.
3. If the observed properties of the data are found to be different from the expected ones, the team responds by rejecting (or fixing) the data, updating the expectations, or both.


Expectations are grouped into Expectations Suites. An Expectation Suite combines multiple expectations into an overall description of a dataset. For example, a team can group all the expectations about the ``rating`` table in the movie ratings database into an Expectation Suite and call it "movieratings.table.expectations".

Each Expectation Suite is saved as a JSON file in the ``great_expectations/expectations`` subdirectory of the Data Context. Users check these files into version control each time they are updated, in the same way they treat their source files.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through a loop of Review and Edit as the team's understanding of the data described by the suite evolves.

We will describe the Create, Review and Edit steps in brief:

Create an Expectation Suite
---------------------------

Expectation Suites are saved as JSON files, so you *could* create a new suite by writing a file directly. However the preferred way is to let the CLI save you time and typos.  If you cannot use the :ref:`CLI <command_line>` in your environment (e.g., in a Databricks cluster), you can create and edit an Expectation Suite in a notebook. Jump to this section for details: :ref:`Jupyter Notebook for Creating and Editing Expectation Suites`.

To continue with the :ref:`CLI <command_line>`, run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


.. code-block:: bash

    great_expectations suite new


This command prompts you to name your new Expectation Suite and to select a sample batch of the dataset the suite will describe. Then it profiles the selected sample and adds some initial expectations to the suite. The purpose of these expectations is to provide examples of what properties of data can be described using Great Expectations. They are only a starting point that the user builds on.

The command concludes by saving the newly generated Expectation Suite as a JSON file and rendering the expectation suite into an HTML page in the Data Docs website of the Data Context.



Review an Expectation Suite
---------------------------

:ref:`Data Docs<data_docs>` is a feature of Great Expectations that creates data documentation by compiling expectations and validation results into HTML.

Data Docs produces a visual data quality report of what you expect from your data, and how the observed properties of your data differ from your expectations.
It helps to keep your entire team on the same page as data evolves.

Reviewing expectations is best done in Data Docs:

.. image:: ../images/sample_e_s_view.png

Edit an Expectation Suite
-------------------------

The best interface for editing an Expectation Suite is a Jupyter notebook.

Editing an Expectation Suite means adding expectations, removing expectations, and modifying the arguments of existing expectations.

For every expectation type there is a Python method that sets its arguments, evaluates this expectation against a sample batch of data and adds it to the Expectation Suite.

Take a look at the screenshot below. It shows the HTML view and the Python method for the same expectation (``expect_column_distinct_values_to_be_in_set``) side by side:

.. image:: ../images/exp_html_python_side_by_side .png

The :ref:`CLI <command_line>` provides a command that, given an Expectation Suite, generates a Jupyter notebook to edit it. It takes care of generating a cell for every expectation in the suite and of getting a sample batch of data. The HTML page for each Expectation Suite has the CLI command syntax in order to make it easier for users.

.. image:: ../images/edit_e_s_popup.png

The generated Jupyter notebook can be discarded, since it is auto-generated.

To understand this auto-generated notebook in more depth, jump to this section: :ref:`Jupyter Notebook for Creating and Editing Expectation Suites`.





Jupyter Notebook for Creating and Editing Expectation Suites
------------------------------------------------------------

If you used the :ref:`CLI <command_line>` `suite new` command to create an Expectation Suite and then the `suite edit` command to edit it, then the CLI generated a notebook in the ``great_expectations/uncommitted/`` folder for you. There is no need to check this notebook in to version control. Next time you decide to
edit this Expectation Suite, use the :ref:`CLI <command_line>` again to generate a new notebook that reflects the expectations in the suite at that time.

If you do not use the :ref:`CLI <command_line>`, create a new notebook in the``great_expectations/notebooks/`` folder in your project.


1. Setup
********************************************

.. code-block:: python

    from datetime import datetime
    import great_expectations as ge
    import great_expectations.jupyter_ux
    from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier

    # Data Context is a GE object that represents your project.
    # Your project's great_expectations.yml contains all the config
    # options for the project's GE Data Context.
    context = ge.data_context.DataContext()

    # Create a new empty Expectation Suite
    # and give it a name
    expectation_suite_name = "ratings.table.warning" # this is just an example
    context.create_expectation_suite(
    expectation_suite_name)


If an expectation suite with this name already exists for this data_asset, you will get an error. If you would like to overwrite this expectation suite, set ``overwrite_existing=True``.


2. Load a batch of data to create Expectations
**********************************************

Select a sample batch of the dataset the suite will describe.

``batch_kwargs`` provide detailed instructions for the Datasource on how to construct a batch. Each Datasource accepts different types of ``batch_kwargs`` - regardless of Datasource type, a Datasource name must always be provided:

.. content-tabs::

    .. tab-container:: tab0
        :title: pandas

        A pandas datasource can accept ``batch_kwargs`` that describe either a path to a file or an existing DataFrame. For example, if the data asset is a collection of CSV files in a folder that are processed with Pandas, then a batch could be one of these files. Here is how to construct ``batch_kwargs`` that specify a particular file to load:

        .. code-block:: python

            batch_kwargs = {
                'path': "PATH_OF_THE_FILE_YOU_WANT_TO_LOAD",
                'datasource': "DATASOURCE_NAME"
            }

        To instruct ``get_batch`` to read CSV files with specific options (e.g., not to interpret the first line as the
        header or to use a specific separator), add them to the ``batch_kwargs`` under the "reader_options" key.

        See the complete list of options for `Pandas read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`__.

        ``batch_kwargs`` might look like the following:

        .. code-block:: json

            {
                "path": "/data/npidata/npidata_pfile_20190902-20190908.csv",
                "datasource": "files_datasource",
                "reader_options": {
                    "sep": "|"
                }
            }

        If you already loaded the data into a Pandas DataFrame called `df`, you could use following ``batch_kwargs`` to instruct the datasource to use your DataFrame as a batch:

        .. code-block:: python

            batch_kwargs = {
                'dataset': df,
                'datasource': 'files_datasource'
            }

    .. tab-container:: tab1
        :title: pyspark

        A pyspark datasource can accept ``batch_kwargs`` that describe either a path to a file or an existing DataFrame. For example, if the data asset is a collection of CSV files in a folder that are processed with Pandas, then a batch could be one of these files. Here is how to construct ``batch_kwargs`` that specify a particular file to load:

        .. code-block:: python

            batch_kwargs = {
                'path': "PATH_OF_THE_FILE_YOU_WANT_TO_LOAD",
                'datasource': "DATASOURCE_NAME"
            }

        To instruct ``get_batch`` to read CSV files with specific options (e.g., not to interpret the first line as the
        header or to use a specific separator), add them to the ``batch_kwargs`` under the "reader_options" key.

        See the complete list of options for `Spark DataFrameReader <https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader>`__

    .. tab-container:: tab2
        :title: SQLAlchemy

        A SQLAlchemy datasource can accept ``batch_kwargs`` that instruct it load a batch from a table, a view, or a result set of a query:

        If you would like to validate an entire table (or a view) in your database's default schema:

        .. code-block:: python

            batch_kwargs = {
                'table': "YOUR TABLE NAME",
                'datasource': "DATASOURCE_NAME"
            }

        If you would like to validate an entire table or view from a non-default schema in your database:

        .. code-block:: python

            batch_kwargs = {
                'table': "YOUR TABLE NAME",
                'schema': "YOUR SCHEMA",
                'datasource': "DATASOURCE_NAME"
            }

        If you would like to validate using a query to construct a temporary table:

        .. code-block:: python

            batch_kwargs = {
                'query': 'SELECT YOUR_ROWS FROM YOUR_TABLE',
                'datasource': "DATASOURCE_NAME"
            }

The DataContext's ``get_batch`` method is used to load a batch of a data asset:

.. code-block:: python

    batch = context.get_batch(batch_kwargs, expectation_suite_name)
    batch.head()

Calling this method asks the Context to get a batch of data and attach the expectation suite ``expectation_suite_name`` to it. The ``batch_kwargs`` argument specifies which batch of the data asset should be loaded.

|
3. Author Expectations
********************************************

Now that you have a batch of data, you can call ``expect`` methods on the data asset in order to check
whether this expectation is true for this batch of data.

For example, to check whether it is reasonable to expect values in the column "NPI" to never be empty, call:
``batch.expect_column_values_to_not_be_null('NPI')``

Some expectations can be created from your domain expertise; for example we might expect that most entries in the NPI
database use the title "Dr." instead of "Ms.", or we might expect that every row should use a unique value in the 'NPI'
column.

Here is how we can add an expectation that expresses that knowledge:

.. image:: ../images/expect_column_values_to_be_unique_success.png

Other expectations can be created by examining the data in the batch. For example, suppose you want to protect a pipeline
against improper values in the "Provider Other Organization Name Type Code" column. Even if you don't know exactly what the
"improper" values are, you can explore the data by trying some values to check if the data in the batch meets your expectation:

.. image:: ../images/expect_column_values_to_be_in_set_failure.png

Validating the expectation against the batch resulted in failure - there are some values in the column that do not meet
the expectation. The "partial_unexpected_list" key in the result dictionary contains examples of non-conforming values.
Examining these examples shows that some titles are not in the expected set. Adjust the ``value_set`` and rerun
the expectation method:

.. image:: ../images/expect_column_values_to_be_in_set_success.png

This time validation was successful - all values in the column meet the expectation.

Although you called ``expect_column_values_to_be_in_set`` twice (with different argument values), only one
expectation of type ``expect_column_values_to_be_in_set`` will be created for the column - the latest call
overrides all the earlier ones. By default, only expectations that were true on their last run are saved.

How do I know which types of expectations I can add?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* *Tab-complete* the partially typed ``expect_`` method name to see available expectations.
* In Jupyter, you can also use *shift-tab* to see the docstring for each expectation, including the parameters it
  takes and to get more information about the expectation.
* Visit the :ref:`expectation_glossary` for a complete
  list of expectations that are currently part of the great expectations vocabulary. Here is a short preview of the glossary:

.. image:: ../images/glossary_of_expectations_preview.png
    :width: 400px



4. Finalize
********************************************

Data Docs compiles Expectations and Validations into HTML documentation. By default the HTML website is hosted on your local filesystem. When you are working in a team, the website can be hosted in the cloud (e.g., on S3) and serve as the shared source of truth for the team working on the data pipeline.

To view the expectation suite you just created as HTML, rebuild the data docs and open the website in the browser:

.. code-block:: python

    # save the Expectation Suite (by default to a JSON file in great_expectations/expectations folder
    batch.save_expectation_suite(discard_failed_expectations=False)

    # This step is optional, but useful - evaluate the expectations against the current batch of data
    run_id = {
      "run_name": "some_string_that_uniquely_identifies_this_run",
      "run_time": datetime.now(datetime.timezone.utc)
    }
    results = context.run_validation_operator("action_list_operator", assets_to_validate=[batch], run_id=run_id)
    expectation_suite_identifier = list(results["details"].keys())[0]
    validation_result_identifier = ValidationResultIdentifier(
        expectation_suite_identifier=expectation_suite_identifier,
        batch_identifier=batch.batch_kwargs.to_id(),
        run_id=run_id
    )

    # Update the Data Docs site to display the new Expectation Suite
    # and open the site in the browser
    context.build_data_docs()
    context.open_data_docs(validation_result_identifier)
