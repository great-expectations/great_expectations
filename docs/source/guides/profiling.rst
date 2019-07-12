.. _profiling:

================================================================================
Profiling
================================================================================

Profiling creates HTML documentation that displays key characteristics of a dataset.


First, it summarizes the variables (columns):

.. image:: movie_db_profiling_screenshot_2.jpg

And then displayes detailed statistics for each column:

.. image:: movie_db_profiling_screenshot_1.jpg

Profiling generates candidate expectations and documentation. The expectations created by the profiling capture a wide range of statistics and other characteristics of a dataset. These describe the dataset effectively and are a useful basis for expectations in the future.

How to Run Profiling
--------------------

Run During Init
~~~~~~~~~~~~~~~~~~~~~~
The init command offers to profile the newly added datasource. If you agree, all data assets in that datasource will be profiled (e.g., all tables in the database).

Profiling is still a beta feature in Great Expectations. The current profiler will evaluate the entire data source (without sampling), which may be very time consuming.
As a rule of thumb, we recommend starting with data smaller than 100MB.

Run From Command Line
~~~~~~~~~~~~~~~~~~~~~~

If you decline profiling during init, you can run it later:

.. code-block::

    great_expectations profile DATASOURCE_NAME

This command will save profiling results as JSON files in 'uncommitted' directory, one file for each data asset. If you want to generate HTML documentations from these results, you have to move the files into 'fixtures/validations':

.. image:: movie_db_profiling_screenshot_3.jpg
    :height: 400px

and then run this command to generate HTML:

.. code-block::

    great_expectations documentation

The HTML documentation can be viewed in a web browser:
great_expectations/uncommitted/documentation/index.html

Run From Jupyter Notebook
~~~~~~~~~~~~~~~~~~~~~~

If you want to profile just one data asset in a datasource (e.g., one table in the database), you can do it using Python in a Jupyter notebook:

.. code-block:: python

    from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
    from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
    from great_expectations.data_context.util import safe_mmkdir
    from great_expectations.render.view import DefaultJinjaPageView

    profiling_html_filepath = 'WHERE YOU WANT TO SAVE THE PROFILING RESULTS HTML FILE'

    # load a batch from the data asset
    batch = context.get_batch('ratings')

    # run the profiler on the batch - this returns an expectation suite and validation results for this suite
    expectation_suite, validation_result = BasicDatasetProfiler.profile(batch)

    # use a renderer to produce a document model from the validation results
    document_model = DescriptivePageRenderer.render(validation_result)

    # use a view to render the document model (produced by the renderer) into a HTML document
    safe_mmkdir(os.path.dirname(profiling_html_filepath))
    with open(profiling_html_filepath, 'w') as writer:
        writer.write(DefaultJinjaPageView.render(document_model))



How Expectations And Profiling Are Related?
-------------------------------------------

In order to characterize a data asset, profiling creates an expectation suite. Unlike the expectations that are typically used for data validation, these expectations do not apply any constraints. This is an example of expect_column_mean_to_be_between expectations that supplies null as values for both min and max. This means that profiling does not expect the mean to be within a particular range - anything is acceptable.

.. code-block::

    {
      "expectation_type": "expect_column_mean_to_be_between",
      "kwargs": {
        "column": "rating",
        "min_value": null,
        "max_value": null
      }
    }

When this expectation is evaluated against a batch, the validation result computes the actual mean and returns it as observed_value. Getting this observed value was the sole purpose of the expectation.

.. code-block::

    {
      "success": true,
      "result": {
        "observed_value": 4.05,
        "element_count": 10000,
        "missing_count": 0,
        "missing_percent": 0
      }
    }



Known Issues
------------

When profiling CSV files, the profiler makes assumptions, such as considering the first line to be the header. Overriding these assumptions is currently possible only when running profiling in Python by passing extra arguments to get_batch.

