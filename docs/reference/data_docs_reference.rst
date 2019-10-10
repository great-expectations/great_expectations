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
      store_backend:
        class_name: FixedLengthTupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
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

``validations_store`` in the example above specifies the location of validation
and profiling results that the site will include in the documentation. The
store's ``type`` can be ``filesystem`` or ``s3`` (S3 store is not currently
implemented, but will be supported in the near future). ``base_directory`` must
be specified for ``filesystem`` stores. The optional ``run_id_filter`` attribute
allows to include (``eq`` for exact match) or exclude (``ne``) validation
results with a particular run id.

*********************
Building Data Docs
*********************

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

.. _customizing_data_docs:

***********************
Customizing Data Docs
***********************

Introduction
=============
Data Docs uses the `Jinja <https://jinja.palletsprojects.com/en/2.10.x/>`_ template engine to generate HTML pages.
The built-in Jinja templates used to compile Data Docs pages are implemented in the :py:mod:`great_expectations.render.view` module
and are tied to *View* classes. Views determine how page content is displayed. The content data that Views
specify and consume is generated by *Renderer* classes and are implemented in the :py:mod:`great_expectations.render.renderer` module.
Renderers take Great Expectations objects as input and return typed dictionaries - Views take these dictionaries as input and
output rendered HTML.

Built-In Views and Renderers
----------------------------
Out of the box, Data Docs supports two top-level Views (i.e. pages), :py:class:`great_expectations.render.view.DefaultJinjaIndexPageView`,
for a site index page, and :py:class:`great_expectations.render.view.DefaultJinjaPageView` for all other pages. Pages
are broken into sections - :py:class:`great_expectations.render.view.DefaultJinjaSectionView` - which are composed of
UI components - :py:class:`great_expectations.render.view.DefaultJinjaComponentView`. Each of these Views references a
single base Jinja template, which can incorporate any number of other templates through inheritance.

Data Docs comes with the following built-in site page Renderers:

  * :py:class:`great_expectations.render.renderer.SiteIndexPageRenderer` (index page)
  * :py:class:`great_expectations.render.renderer.ProfilingResultsPageRenderer` (Profiling Results pages)
  * :py:class:`great_expectations.render.renderer.ExpectationSuitePageRenderer` (Expectation Suite pages)
  * :py:class:`great_expectations.render.renderer.ValidationResultsPageRenderer` (Validation Results pages)

Analogous to the base View templates referenced above, these Renderers can be thought of as base Renderers for the primary
Data Docs pages, and may call on many other ancillary Renderers.

It is possible to extend Renderers and Views and customize the particular class used to render any of the objects in
your documentation.

Other Tools
------------
In addition to Jinja, Data Docs draws on the following libraries to compile HTML documentation, which you can use to
further customize HTML documentation:

  * Bootstrap
  * Font Awesome
  * jQuery
  * Altair

Getting Started
===============
Suppose you start a new Great Expectations project by running ``great_expectations init`` and compile your first Data Docs
site. After looking over the local site, you decide you want to implement the following changes:

  #. A completely new Expectation Suite page, requiring a new View and Renderer
  #. A smaller modification to the default Validation page, swapping out a child renderer for a custom version
  #. Remove Profiling Results pages from the documentation

To make these changes, you must first implement the custom View and Renderers and ensure they are available in the plugins directory specified
in your project configuration (``plugins/`` by default). Note that you can use a subdirectory and standard python submodule
notation, but you must include an __init__.py file in your custom package.

When you are done with your implementations, your ``plugins/`` directory has the following structure:

.. code-block:: bash

  plugins
  ├── custom_renderers
  │   ├── __init__.py
  │   ├── custom_table_renderer.py
  │   └── custom_expectation_suite_page_renderer.py
  └── custom_views
      ├── __init__.py
      └── custom_expectation_suite_view.py

For Data Docs to use your custom Views and Renderers when compiling your local Data Docs site, you must specify
where to use them in the ``data_docs_sites`` section of your project configuration.

Before modifying your project configuration, the relevant section looks like this:

.. code-block:: yaml

  data_docs_sites:
    local_site:
      class_name: SiteBuilder
      store_backend:
        class_name: FixedLengthTupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/

This is what it looks like after your changes are added:

.. code-block:: yaml

    data_docs_sites:
      local_site:
        class_name: SiteBuilder
        store_backend:
          class_name: FixedLengthTupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
        site_section_builders:
          expectations:
            class_name: DefaultSiteSectionBuilder
            source_store_name: expectations_store
            renderer:
              module_name: custom_renderers.custom_expectation_suite_page_renderer
              class_name: CustomExpectationSuitePageRenderer
            view:
              module_name: custom_views.custom_expectation_suite_view
              class_name: CustomExpectationSuiteView
          validations:
            class_name: DefaultSiteSectionBuilder
            source_store_name: validations_store
            run_id_filter:
              ne: profiling
            renderer:
              module_name: great_expectations.render.renderer
              class_name: ValidationResultsPageRenderer
              column_section_renderer:
                class_name: ValidationResultsColumnSectionRenderer
                table_renderer:
                  module_name: custom_renderers.custom_table_renderer
                  class_name: CustomTableRenderer

Note that if your ``data_docs_sites`` configuration contains a ``site_section_builders`` key, you must now explicitly provide
defaults for anything you would like rendered. By omitting the ``profiling`` key within ``site_section_builders``, your third goal
is achieved and Data Docs will no longer render Profiling Results pages.

Lastly, to compile your newly-customized Data Docs local site, you run ``great_expectations build-docs`` from the command line.

``site_section_builders`` defaults:

.. code-block:: yaml

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

Re-Purposing Built-In Views
============================
If you would like to re-purpose a built-in View, you may do so by implementing a custom renderer that outputs an
appropriately typed and structured dictionary for that View.

Built-in Views and corresponding input types:

  * :py:class:`great_expectations.render.view.DefaultJinjaPageView`: :py:class:`great_expectations.render.types.RenderedDocumentContent`

  * :py:class:`great_expectations.render.view.DefaultJinjaSectionView`: :py:class:`great_expectations.render.types.RenderedSectionContent`

  * :py:class:`great_expectations.render.view.DefaultJinjaComponentView`: :py:class:`great_expectations.render.types.RenderedComponentContent`

An example of a custom page Renderer, using all built-in UI elements is provided below.

Custom Page Renderer Example
-----------------------------

.. literalinclude:: ../example_code/custom_renderer.py
    :language: python

.. image:: ../images/customizing_data_docs.png

Dependencies
=============
* Font Awesome 5.10.1
* Bootstrap 4.3.1
* jQuery 3.2.1
* altair 3.1.0
* Vega 5.3.5
* Vega-Lite 3.2.1
* Vega-Embed 4.0.0

