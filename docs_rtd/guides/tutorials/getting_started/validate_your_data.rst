.. _tutorials__getting_started__validate_your_data:

Validate your data using a Checkpoint
=====================================

:ref:`validation` is the core operation of Great Expectations: “Validate data X against Expectation Y.”

In normal usage, the best way to validate data is with a :ref:`Checkpoints <checkpoints_and_actions>`. Checkpoints bundle :ref:`Batches <how_to_guides__creating_batches>` of data with corresponding :ref:`Expectation Suites <how_to_guides__creating_and_editing_expectations>` for validation.

Let’s set up our first Checkpoint to validate the February data! In order to do this, we need to do two things:

1. Define the ``batch_kwargs`` for the February data
2. Configure a Checkpoint to validate that data with our ``taxi.demo`` Expectation Suite.

.. warning::

   As of Great Expectations version 0.13.8 and above, we introduced new style (class-based) Checkpoints. These are **not yet supported by the CLI**, so you will need to configure new Checkpoints in code. We're working on releasing CLI support for Checkpoints very soon!

**Go back to your Jupyter notebook** and add a new cell with the following code:

.. code-block:: python

    # This defines the batch for your February data set
    batch_kwargs_2 = {
        "path": "<path to my code>/ge_tutorials/data/yellow_tripdata_sample_2019-02.csv",
        "datasource": "data__dir",
        "data_asset_name": "yellow_tripdata_sample_2019-02",
    }

    # This is where we configure a Checkpoint to validate the batch with the "taxi.demo" suite
    my_checkpoint = LegacyCheckpoint(
        name="my_checkpoint",
        data_context=context,
        batches=[
            {
              "batch_kwargs": batch_kwargs_2,
              "expectation_suite_names": ["taxi.demo"]
            }
        ]
    )

    # And here we just run validation!
    results = my_checkpoint.run()

**What just happened?**

- ``my_checkpoint`` is the name of your new Checkpoint.
- The Checkpoint uses ``taxi.demo`` as its primary :ref:`Expectation Suite`.
- You configured the Checkpoint to validate the ``yellow_tripdata_sample_2019-02.csv`` file.
- The ``results`` variable now contains the validation results (duh!)

How to save and load a Checkpoint
-----------------------------------

We're currently working on a more user-friendly version of interacting with the new, class-based Checkpoints. In the meantime, here's how you can save your Checkpoint to the Data Context:

.. code-block:: python

    # Save the Checkpoint to your Data Context
    my_checkpoint_json = my_checkpoint.config.to_json_dict()
    context.add_checkpoint(**my_checkpoint_json)

Once you've configured and saved a Checkpoint, you can load and run it every time you want to validate your data. In this example, we're using a named CSV file which you might not want to validate repeatedly. But if you point your Checkpoint at a database table, this will save you a lot of time when running validation on the same table periodically.

.. code-block:: python

    # And here's how you can load it from your Data Context again
    my_loaded_checkpoint = context.get_checkpoint("my_checkpoint")

    # And then run validation again if you'd like
    my_loaded_checkpoint.run()


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
