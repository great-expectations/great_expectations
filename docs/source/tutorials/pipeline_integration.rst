.. _pipeline_integration:

Pipeline Integration
=====================

This tutorial covers integrating Great Expectations (GE) into an existing pipeline.

To continue the example we used in the previous notebook,
we created an expectation suite for the data asset "notable_works_by_charles_dickens". By doing this
we defined what we expect a valid batch of this datato look like.

Once our pipeline is deployed, it will process new batches of this data asset as they arrive as files.

Just before calling the method that does the computation on a new batch, we call Great Expectations'
validate method to make sure that the file meets your expectations about
what a valid orders file should look like.
If the file does not pass validation, we can decide what to do, e.g., stop the pipeline, since its output on invalid input cannot be guaranteed.

To run validation we need 2 things:
* A batch to validate - in our case it is a file loaded into a Pandas Dataframe (or Spark Dataframe, if your pipeline is built on Spark)
* Expectations to validate against




Video
------

This brief video covers the basics of integrating GE into a pipeline.

Get a DataContext object
------------------------

A DataContext represents a Great Expectations project. It organizes storage and access for
expectation suites, datasources, notification settings, and data fixtures.
The DataContext is configured via a yml file stored in a directory called great_expectations;
the configuration file as well as managed expectation suites should be stored in version control.

Obtaining a DataContext object gets us access to these resources after the object reads its
configuration file.

::
    context = ge.data_context.DataContext()

To read more about DataContext.... TODO: insert link

Run Id
-------

A run id links together validations and


Choose Data Asset and Expectation Suite
-----------------------------------------

We called our data asset "notable_works_by_charles_dickens" and created an expectation suite called "node_1"


Obtain a Batch to Validate
-----------------------------

We read the new batch of data from a file that our pipeline is about to process and
convert the resulting Pandas Dataframe into a Great Expectations batch that can be validated.

::
    df = pd.read_csv(file_path_to_validate)
    batch = context.get_batch(data_asset_name, expectation_suite_name, df)

Validate
---------

::
    validation_result = batch.validate(run_id=run_id)

    if validation_result["success"]:
        print("This file meets all expectations from a valid batch of {0:s}".format(data_asset_name))
    else:
        print("This file is not a valid batch of {0:s}".format(data_asset_name))

Review Validation Results
----------------------------

Send Notifications
-------------------

Save Validation Results
-------------------------

Save Failed Batches
---------------------

