.. _data_documentation:

Data Documentation
===================

Data Documentation compiles raw Great Expectations objects including expectation suites and validation reports into
structured documents such as HTML documentation that displays key characteristics of a dataset. Together, Documentation,
Profiling, and Validation are the three core services offered by GE.


Data Documentation is implemented in the :mod:`render` module.

HTML documentation
-------------------

HTML documentation takes expectation suites and validation results and produces clear, functional, and self-healing
documentation of expected and observed data characteristics. Together with profiling, it can help to rapidly create
a clearer picture of your data, and keep your entire team on the same page as data evolves.

For example, the default BasicDatasetProfiler in GE will produce validation_results which compile to a page for each
table or DataFrame including an overview section:

.. image:: movie_db_profiling_screenshot_2.jpg

And then detailed statistics for each column:

.. image:: movie_db_profiling_screenshot_1.jpg


How to build documentation
----------------------------

Using the CLI
~~~~~~~~~~~~~~~

The great_expectations CLI can build comprehensive documentation from expectation suites available to the configured
contxt and validations available in the ``great_expectations/fixtures`` directory.

After building, the he HTML documentation can be viewed in a web browser:

.. code-block:: bash

    open great_expectations/uncommitted/documentation/index.html



Using the raw API
~~~~~~~~~~~~~~~~~~

The underlying python API for rendering documentation is still new and evolving. Use the following snippet as a guide
for how to profile a single batch of data and build documentation from the validation_result.


.. code-block:: python

  from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
  from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
  from great_expectations.data_context.util import safe_mmkdir
  from great_expectations.render.view import DefaultJinjaPageView

  profiling_html_filepath = '/path/into/which/to/save/results'

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
