.. _tutorials__getting_started__create_your_first_expectations:

Create your first Expectations
======================================

:ref:`Expectations` are the key concept in Great Expectations.

Each Expectation is a declarative, machine-verifiable assertion about the expected format, content, or behavior of your data. Great Expectations comes with :ref:`dozens of built-in Expectations <expectation_glossary>`, and it's possible to :ref:`develop your own custom Expectations <how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations>`, too.

.. admonition:: Admonition from Mr. Dickens.

    "Take nothing on its looks; take everything on evidence. There's no better rule."

The CLI will help you create your first Expectation Suite. Suites are simply collections of Expectations. In order to create a new suite, we will use the ``scaffold`` command to automatically create an Expectation Suite called ``taxi.demo`` with the help of a built-in profiler. Type the following into your terminal:

.. code-block:: bash

    great_expectations suite scaffold taxi.demo

You will see the following output:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Would you like to:
        1. choose from a list of data assets in this datasource
        2. enter the path of a data file
    : 1

    Which data would you like to use?
        1. yellow_tripdata_sample_2019-01 (file)
        2. yellow_tripdata_sample_2019-02 (file)
        Don't see the name of the data asset in the list above? Just type it
    : 1

**What just happened?**

You may now wonder why we chose the first file in this step. Here's an explanation: Recall that our data directory contains two CSV files: `yellow_tripdata_sample_2019-01` and `yellow_tripdata_sample_2019-02`.

* `yellow_tripdata_sample_2019-01` contains the 2019 taxi data. Since we want to build an Expectation Suite based on what we know about our taxi data from the January 2019 data set, we want to use it for profiling.
* `yellow_tripdata_sample_2019-02` contains the February 2019 data, which we consider the "new" data set that we want to validate before using it in production. We'll use it *later* when showing you how to validate data.

Makes sense, right?

After selecting the table, Great Expectations will open a Jupyter notebook which will take you through the next part of the ``scaffold`` workflow.

.. warning::

   Don't execute the Jupyter notebook cells just yet!


Creating Expectations in Jupyter notebooks
---------------------------------------------------------

Notebooks are a simple way of interacting with the Great Expectations Python API. You could also just write all this in plain Python code, but for convenience, Great Expectations provides you some boilerplate code in notebooks.

Since notebooks are often less permanent, creating Expectations in a notebook also helps reinforce that the source of truth about Expectations is the Expectation Suite, **not** the code that generates the Expectations.

**Let's take a look through the notebook and see what's happening in each cell:**

#. The first cell does several things: It imports all the relevant libraries, loads a Data Context, and creates what we call a Batch of your data and Expectation Suite.

#. The second cell allows you to specify which columns you want to ignore and which you want to run the automated Profiler on. Remember how we want to add some tests on the ``passenger_count`` column to ensure that its values range between 1 and 6? **Let's comment just this one line to include it:**

    .. code-block:: python

        ignored_columns = [
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            # 'passenger_count',
            ...
        ]

#. The next cell is where you configure a ``UserConfigurableProfiler`` and instantiate it, which will then profile the data and create the relevant Expectations to add to your ``taxi.demo`` suite. You can leave these defaults as-is for now  - :ref:`learn more about the available parameters here. <how_to_guides__creating_and_editing_expectations__how_to_create_an_expectation_suite_with_the_user_configurable_profiler>`

#. The last cell does several things again: It saves the Expectation Suite to disk, runs the validation against the loaded data batch, and then builds and opens Data Docs, so you can look at the validation results. *We will explain the validation step later in the "Validate your data" section.*

**Let's execute all the cells** and wait for Great Expectations to open a browser window with Data Docs. **Go to the next step in the tutorial** for an explanation of what you see in Data Docs!
