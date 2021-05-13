.. _tutorials__getting_started_v3_api__validate_your_data:

Validate your data using a Checkpoint
=====================================

:ref:`validation` is the core operation of Great Expectations: “Validate data X against Expectation Y.”

In normal usage, the best way to validate data is with a :ref:`Checkpoints <checkpoints_and_actions>`. Checkpoints bundle :ref:`Batches <how_to_guides__creating_batches>` of data with corresponding :ref:`Expectation Suites <how_to_guides__creating_and_editing_expectations>` for validation.

Let’s set up our first Checkpoint to validate the February data! Run the following in your terminal:

.. code-block:: bash

    great_expectations --v3-api checkpoint new my_checkpoint

This will open **a Jupyter notebook** that will allow you to complete the configuration of your Checkpoint.


The ``checkpoint new`` notebook
--------------------------------

The Jupyter notebook contains some boilerplate code that allows you to configure a new Checkpoint. The second code cell
is pre-populated with an arbitrarily chosen batch request and Expectation Suite to get you started. Edit it as follows
to configure a Checkpoint to validate the February data:

.. code-block:: python

    my_checkpoint_config = f"""
    name: {my_checkpoint_name}
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
    validations:
      - batch_request:
          datasource_name: taxi_data
          data_connector_name: taxi_data_example_data_connector
          data_asset_name: yellow_tripdata_sample_2019-02.csv
          data_connector_query:
            index: -1
        expectation_suite_name: taxi.demo
    """

You can then execute all cells in the notebook in order to store the Checkpoint to your Data Context.

**What just happened?**

- ``my_checkpoint`` is the name of your new Checkpoint.
- The Checkpoint uses ``taxi.demo`` as its primary :ref:`Expectation Suite`.
- You configured the Checkpoint to validate the ``yellow_tripdata_sample_2019-02.csv`` (i.e. our February data) file.



How to run validation and inspect your validation results
---------------------------------------------------------

This is basically just a recap of the previous section on Data Docs! In order to build Data Docs and get your results
in a nice, human-readable format, you can simply uncomment and run the last cell in the notebook. This will open Data
Docs, where you can click on the latest validation run to see the validation results page for this Checkpoint run.

.. figure:: /images/data_docs_taxi_failed_validation01.png

You'll see that the test suite **failed** when you ran it against the February data. Awesome!

**What just happened? Why did it fail?? Help!?**

We ran the Checkpoint and it successfully failed! **Wait - what?** Yes, that's correct, and that's we wanted.
We know that in this example, the February data has data quality issues, which means we *expect* the validation to fail.

On the validation results page, you will see that the validation of the staging data *failed* because the set
of *Observed Values* in the ``passenger_count`` column contained the value 0.0! This violates our Expectation,
which makes the validation fail.


.. figure:: /images/data_docs_taxi_failed_validation02.png


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
