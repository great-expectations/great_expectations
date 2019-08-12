.. _data_documentation:

Data Documentation
===================

Data Documentation compiles raw Great Expectations objects including expectation suites and validation reports into
structured documents such as HTML documentation that displays key characteristics of a dataset. Together, Documentation,
Profiling, and Validation are the three core services offered by GE.


Data Documentation is implemented in the :py:mod:`great_expectations.render` module.

HTML documentation
-------------------

HTML documentation takes expectation suites and validation results and produces clear, functional, and self-healing
documentation of expected and observed data characteristics. Together with profiling, it can help to rapidly create
a clearer picture of your data, and keep your entire team on the same page as data evolves.

For example, the default BasicDatasetProfiler in GE will produce validation_results which compile to a page for each
table or DataFrame including an overview section:

.. image:: ../images/movie_db_profiling_screenshot_2.jpg

And then detailed statistics for each column:

.. image:: ../images/movie_db_profiling_screenshot_1.jpg


There are three use cases for using documentation in a data project:

1. Visualize all Great Expectations artifacts in the local repo of my project as HTML: expectation suites, validation results and profiling results.

2. Maintain a "shared source of truth" for a team working on a data project. This documentation renders all the artifacts committed in the source control system (expectation suites and profiling results) and a continuously updating data quality report, built from a chronological list of validations by run id.

3. Share a spec of a dataset with a client or a partner. This is similar to an API documentaiton in software development. This documentation would include profiling results of the dataset to give the reader a quick way to grasp what the data looks like, and one or more expectation suites that encode what is expected from the data to be considered valid.


To support these (and possibly other) use cases GE has a concept of "data documentation site". Multiple sites can be configured inside a project, each suitable for a particular data documentation use case.

Here is an example of a site:

.. image:: ../images/data_doc_site_index_page.png

The behavior of a site is controlled by configuration in the DataContext's great_expectations.yml file.

Users can specify

* which datasources to document (by default, all)
* whether to include expectations, validations and profiling results sections
* where the expectations and validations should be read from (filesystem or S3)
* where the HTML files should be written (filesystem or S3)
* which renderer and view class should be used to render each section

Here is an example of a site configuration:

.. code-block:: bash

    data_docs:
      sites:
        local_site: # site name
          type: SiteBuilder
          site_store: # where the HTML will be written to (filesystem/S3)
            type: filesystem
            base_directory: uncommitted/documentation/local_site
          validations_store: # where to look for validation results (filesystem/S3)
            type: filesystem
            base_directory: uncommitted/validations/
            run_id_filter:
              ne: profiling
          profiling_store: # where to look for profiling results (filesystem/S3)
            type: filesystem
            base_directory: uncommitted/validations/
            run_id_filter:
              eq: profiling

          datasources: '*' # by default, all datasources
          sections:
            index:
              renderer:
                module: great_expectations.render.renderer
                class: SiteIndexPageRenderer
              view:
                module: great_expectations.render.view
                class: DefaultJinjaIndexPageView
            validations: # if not present, validation results are not rendered
              renderer:
                module: great_expectations.render.renderer
                class: ValidationResultsPageRenderer
              view:
                module: great_expectations.render.view
                class: DefaultJinjaPageView
            expectations: # if not present, expectation suites are not rendered
              renderer:
                module: great_expectations.render.renderer
                class: ExpectationSuitePageRenderer
              view:
                module: great_expectations.render.view
                class: DefaultJinjaPageView
            profiling: # if not present, profiling results are not rendered
              renderer:
                module: great_expectations.render.renderer
                class: ProfilingResultsPageRenderer
              view:
                module: great_expectations.render.view
                class: DefaultJinjaPageView


By default, GE creates two data documentation sites for a new project:

1. "local_site" renders documentation for all the datasources in the project from GE artifacts in the local repo. The site includes expectation suites and profiling and validation results from `uncommitted` directory. Local site provides the convenience of visualizing all the entities stored in JSON files as HTML.
2. "team_site" is meant to support the "shared source of truth for a team" use case. By default only the expectations section is enabled. Users have to configure the profiling and the validations sections (and the corresponding validations_store and profiling_store attributes based on the team's decisions where these are stored (a local filesystem or S3). Reach out on `Slack <https://tinyurl.com/great-expectations-slack>`__ if you would like to discuss the best way to configure a team site.

How to build documentation
----------------------------

Using the CLI
~~~~~~~~~~~~~~~

The great_expectations CLI can build comprehensive documentation from expectation suites available to the configured
context and validations available in the ``great_expectations/fixtures`` directory.

.. code-block:: bash

    great_expectations documentation


When called without additional arguments, this command will render all the sites specified in great_expectations.yml configuration file.

After building, the HTML documentation can be viewed in a web browser. The command will print out the locations of index.html file for each site.

The sites will not automatically pick up new entities (e.g., a new expectation suite that was added after the last time the site was rendered) - `documentation` command must be called to refresh the site.

To render just one site, use `--site_name SITE_NAME` option.

To render just one data asset (this might be useful for debugging), call

.. code-block:: bash

    great_expectations documentation --site_name SITE_NAME --data_asset_name DATA_ASSET_NAME


Using the raw API
~~~~~~~~~~~~~~~~~~

The underlying python API for rendering documentation is still new and evolving. Use the following snippet as a guide
for how to profile a single batch of data and build documentation from the validation_result.


.. code-block:: python

  from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
  from great_expectations.render.renderer import ProfilingResultsPageRenderer, ExpectationSuitePageRenderer
  from great_expectations.data_context.util import safe_mmkdir
  from great_expectations.render.view import DefaultJinjaPageView

  profiling_html_filepath = '/path/into/which/to/save/results'

  # obtain the DataContext object
  context = ge.data_context.DataContext()

  # load a batch from the data asset
  batch = context.get_batch('ratings')

  # run the profiler on the batch - this returns an expectation suite and validation results for this suite
  expectation_suite, validation_result = BasicDatasetProfiler.profile(batch)

  # use a renderer to produce a document model from the validation results
  document_model = ProfilingResultsPageRenderer.render(validation_result)

  # use a view to render the document model (produced by the renderer) into a HTML document
  safe_mmkdir(os.path.dirname(profiling_html_filepath))
  with open(profiling_html_filepath, 'w') as writer:
      writer.write(DefaultJinjaPageView.render(document_model))
