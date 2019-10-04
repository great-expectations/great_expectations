.. _data_docs_reference:

######################################
Data Docs Reference
######################################

Data Docs make it simple to visualize data quality in your project. These
include Expectations, Validations & Profiles. They are built for all
Datasources from JSON artifacts in the local repo including validations &
profiles from the uncommitted directory.

Users have full control over configuring Data Documentation for their project -
they can modify the pre-configured site (or remove it altogether) and add new
sites with a configuration that meets the project's needs. The easiest way to
add a new site to the configuration is to copy the "local_site" configuration
block in great_expectations.yml, give the copy a new name and modify the details
as needed.

***************************************
Data Docs Site Configuration
***************************************

Here is an example of a site configuration from great_expectations.yml:

.. code-block:: yaml

  data_docs_sites:
    local_site: # site name
      datasource_whitelist: '*' # used to restrict the Datasources
      module_name: great_expectations.render.renderer.site_builder
      class_name: SiteBuilder
      target_store_name: local_site_html_store
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
      site_section_builders:
        expectations: # if not present, expectation suites are not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: expectations_store
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ExpectationSuitePageRenderer

        validations: # if not present, validation results are not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: validations_store
          run_id_filter:
            ne: profiling # exclude validations with run id "profiling" - reserved for profiling results
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ValidationResultsPageRenderer

        profiling: # if not present, profiling results are not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: validations_store
          run_id_filter:
            eq: profiling
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ProfilingResultsPageRenderer

``validations_store`` in the example above specify the location of validation
and profiling results that the site will include in the documentation. The
store's ``type`` can be ``filesystem`` or ``s3`` (S3 store is not currently
implemented, but will be supported in the near future.) ``base_directory`` must
be specified for ``filesystem`` stores. The optional ``run_id_filter`` attribute
allows to include (``eq`` for exact match) or exclude (``ne``) validation
results with a particular run id.

*************************
Building Data Docs
*************************

Using the CLI
===============

The great_expectations CLI can build comprehensive Data Docs from expectation
suites available to the configured context and validations available in the
``great_expectations/uncommitted`` directory.

.. code-block:: bash

    great_expectations build-docs


When called without additional arguments, this command will render all the sites specified in great_expectations.yml configuration file.

After building, the HTML documentation can be viewed in a web browser. The command will print out the locations of index.html file for each site.

The sites will not automatically pick up new entities (e.g., a new expectation suite that was added after the last time the site was rendered) - `documentation` command must be called to refresh the site.

To render just one site, use `--site_name SITE_NAME` option.

To render just one data asset (this might be useful for debugging), call

.. code-block:: bash

    great_expectations build-docs --site_name SITE_NAME --data_asset_name DATA_ASSET_NAME


Using the raw API
===================

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


Dependencies
===============
* Font Awesome 5.10.1
* Bootstrap 4.3.1
* jQuery 3.2.1
* Vega 5.3.5
* Vega-Lite 3.2.1
* Vega-Embed 4.0.0

