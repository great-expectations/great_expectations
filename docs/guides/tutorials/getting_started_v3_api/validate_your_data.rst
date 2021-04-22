.. _tutorials__getting_started_v3_api__validate_your_data:

Validate your data using a Checkpoint
=====================================

:ref:`validation` is the core operation of Great Expectations: “Validate data X against Expectation Y.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Checkpoints bundle :ref:`Batches` of data with corresponding :ref:`Expectation Suites` for validation.

Let’s set up our first Checkpoint to validate the February data! Run the following in your terminal:

.. code-block:: bash

    great_expectations --v3-api checkpoint new my_checkpoint

This will open **a Jupyter notebook** (you guessed it) that will allow you to complete the configuration of your Checkpoint.


The ``checkpoint new`` notebook
--------------------------------

The Jupyter notebook contains some boilerplate code that allows you to configure a new Checkpoint.

TODO: MORE HERE.


**What just happened?**

- ``my_checkpoint`` is the name of your new Checkpoint.
- The Checkpoint uses ``taxi.demo`` as its primary :ref:`Expectation Suite`.
- You configured the Checkpoint to validate the ``yellow_tripdata_sample_2019-02.csv`` file.
- The ``results`` variable now contains the validation results (duh!)




How to inspect your validation results
---------------------------------------

This is basically just a recap of the previous section on Data Docs! In order to build Data Docs and get your results in a nice, human-readable format, you can do the following:

.. code-block:: python

    validation_result_identifier = results.list_validation_result_identifiers()[0]
    context.build_data_docs()
    context.open_data_docs(validation_result_identifier)

Check out the data validation results page that just opened. You'll see that the test suite **failed** when you ran it against the February data. Awesome!

**What just happened? Why did it fail?? Help!?**

We ran the Checkpoint and it successfully failed! **Wait - what?** Yes, that's correct, and that's we wanted. We know that in this example, the February data has data quality issues, which means we *expect* the validation to fail.

.. figure:: /images/validation_results_failed_detail.png

On the validation results page, you will see that the validation of the staging data *failed* because the set of *Observed Values* in the ``passenger_count`` column contained the value 0.0! This violates our Expectation, which makes the validation fail.

If you navigate to the Data Docs *Home* page and refresh, you will also see a *failed* validation run at the top of the page:

.. figure:: /images/validation_results_failed.png


**And this is it!**

We have successfully created an Expectation Suite based on historical data, and used it to detect an issue with our new data. **Congratulations! You have now completed the "Getting started with Great Expectations" tutorial**.

Wrap-up and next steps
-----------------------------

In this tutorial, we have covered the following basic capabilities of Great Expectations:

* Setting up a Data Context
* Connecting a Data Source
* Creating an Expectation Suite using a automated profiling
* Exploring validation results in Data Docs
* Validating a new batch of data with a Checkpoint

As a final, **optional step**, you can check out the next section on how to customize your deployment in order to configure options such as where to store Expectations, validation results, and Data Docs.

And if you want to stop here, feel free to join our `Slack community <https://greatexpectations.io/slack>`_ to say hi to fellow Great Expectations users in the **#beginners** channel!
