.. _tutorials__getting_started__validate_your_data:

Validate your data using a Checkpoint
============================

:ref:`validation` is the core operation of Great Expectations: “Validate data X against Expectation Y.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Checkpoints bring :ref:`Batches` of data together with corresponding :ref:`Expectation Suites` for validation. Configuring Checkpoints simplifies deployment, by pre-specifying the "X"s and "Y"s that you want to validate at any given point in your data infrastructure.

Let’s set up our first Checkpoint by running the following CLI command:

.. code-block:: bash

  great_expectations checkpoint new staging.chk taxi.demo

``staging.chk`` will be the name of your new Checkpoint. It will use ``taxi.demo`` as its primary :ref:`Expectation Suite` and will be configured to validate the ``yellow_tripdata_staging`` table. That way, we can simply run the Checkpoint each time we have new data loaded to ``staging`` and validate that the data meets our expectations!

From there, you will be prompted by the CLI to configure the Checkpoint:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    
    Which table would you like to use? (Choose one)
        1. yellow_tripdata_sample_2019_01 (table)
        2. yellow_tripdata_staging (table)
        Do not see the table in the list above? Just type the SQL query
    2

    A checkpoint named `staging.chk` was added to your project!
    ...
    
Let’s explain what happened there before continuing.

How Checkpoints work
--------------------

Your new Checkpoint file is in ``staging.chk``. With comments removed, it looks like this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          table: yellow_tripdata_staging
          schema: public
          data_asset_name: yellow_tripdata_staging
          datasource: my_postgres_db
        expectation_suite_names:
          - taxi.demo

Our newly configured Checkpoint knows how to load ``yellow_tripdata_staging`` as a Batch, pair it with the ``taxi.demo`` Expectation Suite, and execute validation of the Batch using a pre-configured :ref:`Validation Operator <Validation Operators>` called ``action_list_operator``.

You don't need to worry much about the details of Validation Operators for now. They orchestrate the actual work of validating data and processing the results. After executing validation, the Validation Operator can kick off additional workflows through :ref:`Validation Actions`. For more examples of post-validation actions, please see the :ref:`How-to section for Validation <how_to_guides__validation>`.

How to validate data by running Checkpoints
----------------------

The final step in this tutorial is to confirm that our Expectation Suite indeed catches the data quality issues in the staging data! Run the Checkpoint we just created to trigger validation of the staging data:

.. code-block:: bash

    great_expectations checkpoint run staging.chk

This will output the following:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    Validation Failed!

**What just happened?**

We ran the Checkpoint and it successfully failed! **Wait - what?** Yes, that's correct, and that's we wanted. We know that in this example, the staging data has data quality issues, which means we *expect* the validation to fail. Let's open up Data Docs again to see the details.

If you refresh the Data Docs Home page, you will now see a *failed* validation result at the top of the page:

.. figure:: /images/validation_results_failed.png

If you click through to the failed validation results page, you will see that the validation of the staging data *failed* because the set of *Observed Values* in the ``passenger_count`` column contained the value 0! This violates our Expectation, which makes the validation fail.

.. figure:: /images/validation_results_failed_detail.png

**And this is it!** We have successfully created an Expectation Suite based on historical data, and used it to detect an issue with our new data.

Wrap-up and next steps
----------------------

**Congratulations! You have now completed the "Getting started with Great Expectations" tutorial**. In this tutorial, we have covered the following basic capabilities of Great Expectations:

* Setting up a Data Context
* Connecting a Data Source
* Creating an Expectation Suite using a automated profiling
* Exploring validation results in Data Docs
* Validating a new batch of data with a Checkpoint

As a final, optional step, you can check out the next section on how to customize your deployment in order to configure options such as where to store Expectations, validation results, and Data Docs.