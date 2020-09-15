.. _tutorials__getting_started__create_your_first_expectations:

Create your first Expectations
======================================

:ref:`Expectations` are the key concept in Great Expectations.

Each Expectation is a declarative, machine-verifiable assertion about the expected format, content, or behavior of your data. Great Expectations comes with :ref:`dozens of built-in Expectations <expectation_glossary>`, and it's possible to :ref:`develop your own custom Expectations <how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations>`, too.

.. admonition:: Admonition from Mr. Dickens.

    "Take nothing on its looks; take everything on evidence. There's no better rule."

The CLI will help you create your first Expectation Suite. :ref:`expectation_suites` are simply collections of Expectations. In order to create a new suite, we will use the ``scaffold`` command to automatically create an Expectation Suite called ``taxi.demo`` with the help of a built-in profiler. Type the following into your terminal:

.. code-block:: bash

    great_expectations suite scaffold taxi.demo

You will see the following output:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Which table would you like to use? (Choose one)
        1. yellow_tripdata_sample_2019_01 (table)
        2. yellow_tripdata_staging (table)
        Do not see the table in the list above? Just type the SQL query
    : 1

**What just happened?**

You may now wonder why we chose the first table in this step. Here's an explanation: Recall that our database contains two pre-loaded tables: `yellow_tripdata_sample_2019_01` and `yellow_tripdata_staging`.

* `yellow_tripdata_sample_2019_01` contains the 2019 taxi data. Since we want to build an Expectation Suite based on what we know about our taxi data from the January 2019 data set, we want to use it for profiling.
* `yellow_tripdata_staging` contains the February 2019 data, loaded to a staging table that we want to validate before promoting it to production. We'll use it *later* when showing you how to validate data.

Makes sense, right?

After selecting the table, Great Expectations will open a Jupyter notebook which will take you through the next part of the ``scaffold`` workflow.

.. warning::

   Don't execute the Jupyter notebook cells just yet!


Creating Expectations in Jupyter notebooks
---------------------------------------------------------

Notebooks are a simple way of interacting with the Great Expectations Python API. You could also just write all this in plain Python code, but for convenience, Great Expectations provides you some boilerplate code in notebooks.

Since notebooks are often less permanent, creating Expectations in a notebook also helps reinforce that the source of truth about Expectations is the Expectation Suite, **not** the code that generates the Expectations.


.. figure:: /images/jupyter_scaffold.gif


**Let's scroll through the notebook and see what's happening in each cell:**

#. The first cell does several things: It imports all the relevant libraries, loads a Data Context, and creates what we call a Batch of your data and Expectation Suite.

#. The second cell allows you to specify which columns you want to run the automated Profiler on. Remember how we want to add some tests on the ``passenger_count`` column to ensure that its values range between 1 and 6? **Let's uncomment just this one line:**

    .. code-block:: python

        included_columns = [
            # 'vendor_id',
            # 'pickup_datetime',
            # 'dropoff_datetime',
            'passenger_count',
            ...
        ]

#. The next cell passes the Profiler config to the ``BasicSuiteBuilderProfiler``, which will then profile the data and create the relevant Expectations to add to your ``taxi.demo`` suite.

#. The last cell does several things again: It saves the Expectation Suite to disk, runs the validation against the loaded data batch, and then builds and opens Data Docs, so you can look at the validation results.

**Let's execute all the cells** and wait for Great Expectations to open a browser window with Data Docs. **Pause here** to read on first and find out what just happened!


.. _tutorials__getting_started__create_your_first_expectations__what_just_happened:

What just happened?
-------------------

You can create and edit Expectations using several different workflows. The CLI just used one of the quickest and simplest: scaffolding Expectations using an automated :ref:`Profiler <Profilers>`.

This Profiler connected to your data (using the Datasource you configured in the previous step), took a quick look at the contents, and produced an initial set of Expectations. These Expectations are not intended to be very smart. Instead, the goal is to quickly provide some good examples, so that you're not starting from a blank slate.

Later, you should also take a look at other workflows for :ref:`Creating and editing Expectations <how_to_guides__creating_and_editing_expectations>`. Creating and editing Expectations is a very active area of work in the Great Expectations community. Stay tuned for improvements over time.


A first look at real Expectations
---------------------------------

The newly profiled Expectations are stored in an :ref:`Expectation Suite <reference__core_concepts__expectations__expectation_suites>`.

By default, Expectation Suites are stored in a JSON file in a subdirectory of your ``great_expectations/`` folder. You can also configure Great Expectations to store Expectations to other locations, such as S3, Postgres, etc. We'll come back to these options in the last step of the tutorial.

If you open up the file at ``great_expectations/expectations/taxi/demo.json`` in a text editor, you'll see the following:

.. code-block::

    {
      "data_asset_type": "Dataset",
      "expectation_suite_name": "taxi.demo",
      "expectations": [

        ...

        {
          "expectation_type": "expect_column_values_to_not_be_null",
          "kwargs": {
            "column": "passenger_count"
          },
          "meta": {
            "BasicSuiteBuilderProfiler": {
              "confidence": "very low"
            }
          }
        },
        {
          "expectation_type": "expect_column_distinct_values_to_be_in_set",
          "kwargs": {
            "column": "passenger_count",
            "value_set": [
              1.0,
              2.0,
              3.0,
              4.0,
              5.0,
              6.0
            ]
          },
          "meta": {
            "BasicSuiteBuilderProfiler": {
              "confidence": "very low"
            }
          }
        },
        ...

There's a lot of information in the JSON file. **We will focus on just the snippet above:**

Every Expectation in the file expresses a test that can be validated against data. You can see that the Profiler generated several Expectations based on our data, including ``expect_column_distinct_values_to_be_in_set``, with the ``value_set`` containing the numbers 1 through 6. This is exactly what we wanted: An assertion that the ``passenger_count`` column contains only those values!

**Now we only have two problems left to solve:**

#. These dense JSON objects are very hard to read. How can we have a nicer representation of our Expectations?
#. How do we use this Expectation Suite to validate that new batch of data we have in our ``staging`` table?

**Let's execute all the cells** and wait for Great Expectations to open a browser window with Data Docs. **Go to the next step in the tutorial** for an explanation of what you see in Data Docs!
