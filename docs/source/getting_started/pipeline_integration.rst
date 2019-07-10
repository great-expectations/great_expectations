.. _tutorial_pipeline_integration:

Step 3: Pipeline integration
=================================

This tutorial covers integrating Great Expectations (GE) into an existing pipeline.

We will continue the example we used in the previous sect, where we created an expectation suite for the data
asset ``notable_works_by_charles_dickens``. That defined what we expect a valid batch of this data to look like.

Once our pipeline is deployed, it will process new batches of this data asset as they arrive as files. We will use
Great Expectations to validate each batch and ensure it matches our expectations for the relevant component of our
data application.

Just before calling the method that does the computation on a new batch, we call Great Expectations' validate method.
If the file does not pass validation, we can decide what to do--stop the pipeline, log a warning, send a notification
or perform some other custom action.

A Great Expectations DataContext describes data assets using a three-part namespace consisting of:
**datasource_name**, **generator_name**, and **generator_asset**.

To run validation for a data_asset, we need two additional elements:

* A batch to validate - in our case it is a file loaded into a Pandas DataFrame
* Expectations to validate against


                                    ---------------------
                                    |  datasource_name  |
                                    ---------------------
                                              |
                                    ---------------------
                                    |   generator_name  |
                                    ---------------------
                                              |
                                    ---------------------
                                    |  generator_asset  |
                                    ---------------------
                                        /           \
                                       /             \
                                      /               \
                        ---------------------   ---------------------
                        |      batch_id     |   | expectation_suite |
                        ---------------------   ---------------------


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

To read more about DataContext, see: :ref:`DataContext`_

Run Id
-------

A ``run_id`` links together validations of different data_assets, making it possible to track "runs" of a pipeline and
follow data assets as they are transformed, joined, annotated, enriched, or evaluated. The run id can be any string;
by default, Great Expectations will use an ISO 8601-formatted UTC datetime string.

The Great Expectations DataContext object uses the run id to determine when :ref:`evaluation_parameters`_ should be
linked between data assets. After each validation completes, the DataContext identifies and stores any validation
results that are referenced by other data asset expectation suites managed by the context. Then, when a batch of data
is validated against one of those expectation suites, *with the same run id*, the context will automatically insert
the relevant parameter from the validation result. For example, if a batch of the ``node_2`` data_asset expects the
number of unique values in its ``id`` column to equal the number of distinct values in the ``id`` column from
``node_1``, we need to provide the same run_id to ensure that parameter can be passed between validations.

::

    run_id = run_id = datetime.datetime.utcnow().isoformat()


Choose Data Asset and Expectation Suite
-----------------------------------------

We called our data asset "notable_works_by_charles_dickens" and created an expectation suite called "default". Since
"default" is the default name, we can omit that parameter. For our validation, we will link all of the parameters and
supply them to great_expectations:


Obtain a Batch to Validate
-----------------------------

Datasources and generators work together closely with your pipeline infrastructure to provide Great Expectations
batches of data to validate. The generator is responsible for identifying the ``batch_kwargs`` that a datasource will
use to load a batch of data. For example the :py:class:`great_expectations.datasource.generator.filesystem_path_generator.SubdirReaderGenerator`_
generator will create batches of data based on individual files and group those batches into a single data_asset based
on the subdirectory in which they are located. By contrast, the :py:class:`great_expectations.datasource.generator.filesystem_path_generator.GlobReaderGenerator`_
will also create batches of data based on individual files, but uses defined glob-style match patterns to group those
batches into named data assets.

``batch_kwargs`` from one of those filesystem reader generators might look like the following:

::

  {
    "path": "/data/staging/user_actions/20190710T034323_user_actions.csv"
    "timestamp": 1562770986.6800103
  }

Notice that the

We read the new batch of data from a file that our pipeline is about to process and
convert the resulting Pandas DataFrame into a Great Expectations batch that can be validated.

::

    df = pd.read_csv(file_path_to_validate)
    batch = context.get_batch(data_asset_name, expectation_suite_name, df)

Validate
---------

::

    validation_result = ge.validate(df,
          data_context=context,
          data_asset_name="data/default/notable_works_by_charles_dickens",
          run_id=run_id
          )


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

