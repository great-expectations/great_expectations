.. _data_docs_reference:

.. warning:: This doc is spare parts: leftover pieces of old documentation.
  It's potentially helpful, but may be incomplete, incorrect, or confusing.

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

.. _data_docs_site_configuration:

***************************************
Data Docs Site Configuration
***************************************

The default Data Docs site configuration looks like this:

.. code-block:: yaml

  data_docs_sites:
    local_site:
      class_name: SiteBuilder
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
      site_index_builder:
        class_name: DefaultSiteIndexBuilder

Here is an example of a site configuration from great_expectations.yml with defaults defined explicitly:

.. code-block:: yaml

  data_docs_sites:
    local_site: # site name
      module_name: great_expectations.render.renderer.site_builder
      class_name: SiteBuilder
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
      site_section_builders:
        expectations:  # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: expectations_store
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ExpectationSuitePageRenderer

        validations:  # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: validations_store
          run_name_filter:
            ne: profiling # exclude validations with run_name "profiling" - reserved for profiling results
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ValidationResultsPageRenderer

        profiling:  # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
          class_name: DefaultSiteSectionBuilder
          source_store_name: validations_store
          run_name_filter:
            eq: profiling # include ONLY validations with run_name "profiling" - reserved for profiling results
          renderer:
            module_name: great_expectations.render.renderer
            class_name: ProfilingResultsPageRenderer

``validations_store`` in the example above specifies the name of a store configured in the `stores` section.
Validation and profiling results from that store will be included in the documentation. The optional ``run_name_filter``
attribute allows to include (``eq`` for exact match) or exclude (``ne``) validation results with a particular run name.

.. _customizing_data_docs_store_backend:

Limiting Validation Results
============================

If you would like to limit rendered Validation Results to the n most-recent (per Expectation Suite), you may
do so by setting the `validation_results_limit` key in your Data Docs configuration:

.. code-block:: yaml

  data_docs_sites:
    local_site:
      class_name: SiteBuilder
      show_how_to_buttons: true
      store_backend:
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
      site_index_builder:
        class_name: DefaultSiteIndexBuilder
        validation_results_limit: 5

Automatically Publishing Data Docs
=====================================

It is possible to directly publish (continuously updating!) data docs sites to a shared location such as a static
site hosted in S3 by simply updating the store_backend configuration in the site configuration. If we modify the
configuration in the example above to adjust the store backend to an S3 bucket of our choosing, our `SiteBuilder`
will automatically save the resulting site to that bucket.

.. code-block:: yaml

  store_backend:
    class_name: TupleS3StoreBackend
    bucket: data-docs.my_org.org
    prefix:

See the tutorial on `how_to_guides__configuring_data_docs__how_to_host_and_share_data_docs_on_s3` for more information.

More advanced configuration
============================

It is possible to extend renderers and views and customize the particular class used to render any of the objects
in your documentation. In this more advanced configuration, a "CustomTableContentBlockRenderer" is used only for
the validations renderer, and no profiling results are rendered at all.

.. code-block:: yaml

    data_docs_sites:
      # Data Docs make it simple to visualize data quality in your project. These
      # include Expectations, Validations & Profiles. The are built for all
      # Datasources from JSON artifacts in the local repo including validations &
      # profiles from the uncommitted directory. Read more at
      # https://docs.greatexpectations.io/en/latestfeatures/data_docs.html
      local_site:
        class_name: SiteBuilder
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
        site_section_builders:
          validations:  # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
            class_name: DefaultSiteSectionBuilder
            source_store_name: validations_store
            run_name_filter:
              ne: profiling
            renderer:
              module_name: great_expectations.render.renderer
              class_name: ValidationResultsPageRenderer
              column_section_renderer:
                class_name: ValidationResultsColumnSectionRenderer
                table_renderer:
                  module_name: custom_renderers.custom_table_content_block
                  class_name: CustomTableContentBlockRenderer

          profiling:  # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered

To support that custom renderer, we need to ensure the implementation is available in our plugins/ directory.
Note that we can use a subdirectory and standard python submodule notation, but that we need to include an __init__.py
file in our custom_renderers package.

.. code-block:: bash

    plugins/
    ├── custom_renderers
    │   ├── __init__.py
    │   └── custom_table_content_block.py
    └── additional_ge_plugin.py


*********************
Building Data Docs
*********************

Using the CLI
===============

The :ref:`Great Expectations CLI <command_line>` can build comprehensive Data Docs from expectation
suites available to the configured context and validations available in the
``great_expectations/uncommitted`` directory.

.. code-block:: bash

    great_expectations docs build


When called without additional arguments, this command will render all the Data
Docs sites specified in great_expectations.yml configuration file into HTML and
open them in a web browser.

The command will print out the locations of index.html file for each site.

To disable the web browser opening behavior use `--no-view` option.

To render just one site, use `--site-name SITE_NAME` option.

Here is when the ``docs build`` command should be called:

* when you want to fully rebuild a Data Docs site
* after a new expectation suite is added or an existing one is edited
* after new data is profiled (only if you declined the prompt to build data docs when running the profiling command)

When a new validation result is generated after running a Validation Operator, the Data Docs sites will add this result automatically if the operator has the UpdateDataDocsAction action configured (read :ref:`Validation Actions <validation_actions>`).


Using the raw API
===================

The underlying python API for rendering documentation is still new and evolving. Use the following snippet as a guide
for how to profile a single batch of data and build documentation from the validation_result.


.. code-block:: python

  import os
  import great_expectations as ge

  from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
  from great_expectations.render.renderer import ProfilingResultsPageRenderer, ExpectationSuitePageRenderer
  from great_expectations.render.view import DefaultJinjaPageView

  profiling_html_filepath = '/path/into/which/to/save/results.html'

  # obtain the DataContext object
  context = ge.data_context.DataContext()

  # load a batch to profile
  context.create_expectation_suite('default')
  batch = context.get_batch(
    batch_kwargs=context.build_batch_kwargs("my_datasource", "my_batch_kwargs_generator", "my_asset")
    expectation_suite_name='default',
  )

  # run the profiler on the batch - this returns an expectation suite and validation results for this suite
  expectation_suite, validation_result = BasicDatasetProfiler().profile(batch)

  # use a renderer to produce a document model from the validation results
  document_model = ProfilingResultsPageRenderer().render(validation_result)

  # use a view to render the document model (produced by the renderer) into a HTML document
  os.makedirs(os.path.dirname(profiling_html_filepath), exist_ok=True)
  with open(profiling_html_filepath, 'w') as writer:
      writer.write(DefaultJinjaPageView().render(document_model))

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

Making Minor Adjustments
-------------------------
Many of the HTML elements in the default Data Docs pages have pre-configured classes that you may use to make
minor adjustments using your own custom CSS. By default, when you run ``great_expectations init``,
Great Expectations creates a scaffold within the ``plugins`` directory for customizing Data Docs.
Within this scaffold is a file called ``data_docs_custom_styles.css`` - this CSS file contains all the pre-configured classes
you may use to customize the look and feel of the default Data Docs pages. All custom CSS, applied to these pre-configured classes
or any other HTML elements, must be placed in this file.

Scaffolded directory tree:

.. code-block:: bash

  plugins
  └── custom_data_docs
      ├── renderers
      ├── styles
      │   └── data_docs_custom_styles.css
      └── views

Using Custom Views and Renderers
---------------------------------
Suppose you start a new Great Expectations project by running ``great_expectations init`` and compile your first Data Docs
site. After looking over the local site, you decide you want to implement the following changes:

  #. A completely new Expectation Suite page, requiring a new View and Renderer
  #. A smaller modification to the default Validation page, swapping out a child renderer for a custom version
  #. Remove Profiling Results pages from the documentation

To make these changes, you must first implement the custom View and Renderers and ensure they are available in the plugins directory specified
in your project configuration (``plugins/`` by default). Note that you can use a subdirectory and standard python submodule
notation, but you must include an __init__.py file in your custom package. By default, when you run ``great_expectations init``,
Great Expectations creates placeholder directories for your custom views, renderers, and CSS stylesheets within the ``plugins`` directory. If you wish, you may save
your custom views and renderers in an alternate location however, any CSS stylesheets must be saved to ``plugins/custom_data_docs/styles``.

Scaffolded directory tree:

.. code-block:: bash

  plugins
  └── custom_data_docs
      ├── renderers
      ├── styles
      │   └── data_docs_custom_styles.css
      └── views

When you are done with your implementations, your ``plugins/`` directory has the following structure:

.. code-block:: bash

  plugins
  └── custom_data_docs
      ├── renderers
          ├── __init__.py
          ├── custom_table_renderer.py
          └── custom_expectation_suite_page_renderer.py
      ├── styles
      │   └── data_docs_custom_styles.css
      └── views
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
        class_name: TupleFilesystemStoreBackend
        base_directory: uncommitted/data_docs/local_site/
      site_index_builder:
        class_name: DefaultSiteIndexBuilder

This is what it looks like after your changes are added:

.. code-block:: yaml

    data_docs_sites:
      local_site:
        class_name: SiteBuilder
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
        site_section_builders:
          expectations:
            renderer:
              module_name: custom_data_docs.renderers.custom_expectation_suite_page_renderer
              class_name: CustomExpectationSuitePageRenderer
            view:
              module_name: custom_data_docs.views.custom_expectation_suite_view
              class_name: CustomExpectationSuiteView
          validations:
            renderer:
              module_name: great_expectations.render.renderer
              class_name: ValidationResultsPageRenderer
              column_section_renderer:
                class_name: ValidationResultsColumnSectionRenderer
                table_renderer:
                  module_name: custom_data_docs.renderers.custom_table_renderer
                  class_name: CustomTableRenderer
          profiling:

By providing an empty ``profiling`` key within ``site_section_builders``, your third goal
is achieved and Data Docs will no longer render Profiling Results pages. The same can be achieved by setting the \
``profiling`` key to any of the following values: ``['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE']``.

Lastly, to compile your newly-customized Data Docs local site, you run ``great_expectations docs build`` from the command line.

``site_section_builders`` defaults:

.. code-block:: yaml

    site_section_builders:
      expectations: # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
        class_name: DefaultSiteSectionBuilder
        source_store_name: expectations_store
        renderer:
          module_name: great_expectations.render.renderer
          class_name: ExpectationSuitePageRenderer

      validations: # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
        class_name: DefaultSiteSectionBuilder
        source_store_name: validations_store
        run_name_filter:
          ne: profiling # exclude validations with run_name "profiling" - reserved for profiling results
        renderer:
          module_name: great_expectations.render.renderer
          class_name: ValidationResultsPageRenderer

      profiling: # if empty, or one of ['0', 'None', 'False', 'false', 'FALSE', 'none', 'NONE'], section not rendered
        class_name: DefaultSiteSectionBuilder
        source_store_name: validations_store
        run_name_filter:
          eq: profiling # include ONLY validations with run_name "profiling" - reserved for profiling results
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

.. literalinclude:: ../../example_code/custom_renderer.py
    :language: python

.. image:: /images/customizing_data_docs.png

Dependencies
=============
* Font Awesome 5.10.1
* Bootstrap 4.3.1
* jQuery 3.2.1
* altair 3.1.0
* Vega 5.3.5
* Vega-Lite 3.2.1
* Vega-Embed 4.0.0

Data Docs is implemented in the :py:mod:`great_expectations.render` module.
