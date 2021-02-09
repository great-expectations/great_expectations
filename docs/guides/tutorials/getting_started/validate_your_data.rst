.. _tutorials__getting_started__validate_your_data:

Validate your data using a Checkpoint
=====================================

:ref:`validation` is the core operation of Great Expectations: “Validate data X against Expectation Y.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Checkpoints bundle :ref:`Batches` of data with corresponding :ref:`Expectation Suites` for validation.

Let’s set up our first Checkpoint! **Go back to your terminal** and shut down the Jupyter notebook, if you haven't yet. Then run the following command:

.. code-block:: bash

  great_expectations checkpoint new my_checkpoint taxi.demo

From there, you will be prompted by the CLI to configure the Checkpoint:

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

    A checkpoint named `my_checkpoint` was added to your project!
          - To run this checkpoint run `great_expectations checkpoint run my_checkpoint`

**What just happened?**

- ``my_checkpoint`` is the name of your new Checkpoint.
- The Checkpoint uses ``taxi.demo`` as its primary :ref:`Expectation Suite`.
- You configured the Checkpoint to validate the ``yellow_tripdata_sample_2019-02`` file.

How to validate data by running Checkpoints
--------------------------------------------------

The final step in this tutorial is to use our Expectation Suite to alert us of the 0 values in the ``passenger_count`` column! Run the Checkpoint we just created to trigger validation of the new dataset:

.. code-block:: bash

    great_expectations checkpoint run my_checkpoint

This will output the following:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    Validation failed!

**What just happened? Why did it fail?? Help!?**

We ran the Checkpoint and it successfully failed! **Wait - what?** Yes, that's correct, and that's we wanted. We know that in this example, the February data has data quality issues, which means we *expect* the validation to fail. Let's open up Data Docs again to see the details.

If you navigate to the Data Docs *Home* page and refresh, you will now see a *failed* validation run at the top of the page:

.. figure:: /images/validation_results_failed.png

If you click through to the validation results page, you will see that the validation of the staging data *failed* because the set of *Observed Values* in the ``passenger_count`` column contained the value 0.0! This violates our Expectation, which makes the validation fail.

.. figure:: /images/validation_results_failed_detail.png

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