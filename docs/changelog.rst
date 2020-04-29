.. _changelog:

#########
Changelog
#########

develop
-----------------
* new cli command noun `checkpoint` (in the next major release this tap will be deprecated
* improvements to ge.read_json tests
* tidy up the changelog

  - Fix bullet list spacing issues
  - Fix 0.10. formatting
  - Drop roadmap_and_changelog.rst and move changelog.rst to the top level of the table of contents
* DataContext.run_validation_operator() now raises a DataContextError if:
  - no batches are passed
  - batches are of the the wrong type
  - no matching validation operator is found in the project
* Clarified scaffolding language in scaffold notebook
* DataContext.create() adds an additional directory: `checkpoints`

0.10.4
-----------------
* consolidated error handling in CLI DataContext loading
* new cli command `suite scaffold` to speed up creation of suites
* new cli command `suite demo` that creates an example suite
* Update bigquery.rst `#1330 <https://github.com/great-expectations/great_expectations/issues/1330>`_
* Fix datetime reference in create_expectations.rst `#1321 <https://github.com/great-expectations/great_expectations/issues/1321>`_ Thanks @jschendel !
* Update issue templates
* CLI command experimental decorator
* Update style_guide.rst
* Add pull request template
* Use pickle to generate hash for dataframes with unhashable objects. `#1315 <https://github.com/great-expectations/great_expectations/issues/1315>`_ Thanks @shahinism !
* Unpin pytest

0.10.3
-----------------
* Use pickle to generate hash for dataframes with unhashable objects.

0.10.2
-----------------
* renamed NotebookRenderer to SuiteEditNotebookRenderer
* SuiteEditNotebookRenderer now lints using black
* New SuiteScaffoldNotebookRenderer renderer to expedite suite creation
* removed autopep8 dependency
* bugfix: extra backslash in S3 urls if store was configured without a prefix `#1314 <https://github.com/great-expectations/great_expectations/issues/1314>`_

0.10.1
-----------------
* removing bootstrap scrollspy on table of contents `#1282 <https://github.com/great-expectations/great_expectations/issues/1282>`_
* Silently tolerate connection timeout during usage stats reporting

0.10.0
-----------------
* (BREAKING) Clarified API language: renamed all ``generator`` parameters and methods to the more correct ``batch_kwargs_generator`` language. Existing projects may require simple migration steps. See :ref:`Upgrading to 0.10.x` for instructions.
* Adds anonymized usage statistics to Great Expectations. See this article for details: :ref:`Usage Statistics`.
* CLI: improve look/consistency of ``docs list``, ``suite list``, and ``datasource list`` output; add ``store list`` and ``validation-operator list`` commands.
* New SuiteBuilderProfiler that facilitates faster suite generation by allowing columns to be profiled
* Added two convenience methods to ExpectationSuite: get_table_expectations & get_column_expectations
* Added optional profiler_configuration to DataContext.profile() and DataAsset.profile()
* Added list_available_expectation_types() to DataAsset

0.9.11
-----------------
* Add evaluation parameters support in WarningAndFailureExpectationSuitesValidationOperator `#1284 <https://github.com/great-expectations/great_expectations/issues/1284>`_ thanks `@balexander <https://github.com/balexander>`_
* Fix compatibility with MS SQL Server. `#1269 <https://github.com/great-expectations/great_expectations/issues/1269>`_ thanks `@kepiej <https://github.com/kepiej>`_
* Bug fixes for query_generator `#1292 <https://github.com/great-expectations/great_expectations/issues/1292>`_ thanks `@ian-whitestone <https://github.com/ian-whitestone>`_

0.9.10
-----------------
* Data Docs: improve configurability of site_section_builders
* TupleFilesystemStoreBackend now ignore `.ipynb_checkpoints` directories `#1203 <https://github.com/great-expectations/great_expectations/issues/1203>`_
* bugfix for Data Docs links encoding on S3 `#1235 <https://github.com/great-expectations/great_expectations/issues/1235>`_

0.9.9
-----------------
* Allow evaluation parameters support in run_validation_operator
* Add log_level parameter to jupyter_ux.setup_notebook_logging.
* Add experimental display_profiled_column_evrs_as_section and display_column_evrs_as_section methods, with a minor (nonbreaking) refactor to create a new _render_for_jupyter method.
* Allow selection of site in UpdateDataDocsAction with new arg target_site_names in great_expectations.yml
* Fix issue with regular expression support in BigQuery (#1244)

0.9.8
-----------------
* Allow basic operations in evaluation parameters, with or without evaluation parameters.
* When unexpected exceptions occur (e.g., during data docs rendering), the user will see detailed error messages, providing information about the specific issue as well as the stack trace.
* Remove the "project new" option from the command line (since it is not implemented; users can only run "init" to create a new project).
* Update type detection for bigquery based on driver changes in pybigquery driver 0.4.14. Added a warning for users who are running an older pybigquery driver
* added execution tests to the NotebookRenderer to mitigate codegen risks
* Add option "persist", true by default, for SparkDFDataset to persist the DataFrame it is passed. This addresses #1133 in a deeper way (thanks @tejsvirai for the robust debugging support and reproduction on spark).

  * Disabling this option should *only* be done if the user has *already* externally persisted the DataFrame, or if the dataset is too large to persist but *computations are guaranteed to be stable across jobs*.

* Enable passing dataset kwargs through datasource via dataset_options batch_kwarg.
* Fix AttributeError when validating expectations from a JSON file
* Data Docs: fix bug that was causing erratic scrolling behavior when table of contents contains many columns
* Data Docs: add ability to hide how-to buttons and related content in Data Docs

0.9.7
-----------------
* Update marshmallow dependency to >3. NOTE: as of this release, you MUST use marshamllow >3.0, which REQUIRES python 3. (`#1187 <https://github.com/great-expectations/great_expectations/issues/1187>`_) @jcampbell

  * Schema checking is now stricter for expectation suites, and data_asset_name must not be present as a top-level key in expectation suite json. It is safe to remove.
  * Similarly, datasource configuration must now adhere strictly to the required schema, including having any required credentials stored in the "credentials" dictionary.

* New beta CLI command: `tap new` that generates an executable python file to expedite deployments. (`#1193 <https://github.com/great-expectations/great_expectations/issues/1193>`_) @Aylr
* bugfix in TableBatchKwargsGenerator docs
* Added feature maturity in README (`#1203 <https://github.com/great-expectations/great_expectations/issues/1203>`_) @kyleaton
* Fix failing test that should skip if postgresql not running (`#1199 <https://github.com/great-expectations/great_expectations/issues/1199>`_) @cicdw


0.9.6
-----------------
* validate result dict when instantiating an ExpectationValidationResult (`#1133 <https://github.com/great-expectations/great_expectations/issues/1133>`_)
* DataDocs: Expectation Suite name on Validation Result pages now link to Expectation Suite page
* `great_expectations init`: cli now asks user if csv has header when adding a Spark Datasource with csv file
* Improve support for using GCP Storage Bucket as a Data Docs Site backend (thanks @hammadzz)
* fix notebook renderer handling for expectations with no column kwarg and table not in their name (`#1194 <https://github.com/great-expectations/great_expectations/issues/1194>`_)


0.9.5
-----------------
* Fixed unexpected behavior with suite edit, data docs and jupyter
* pytest pinned to 5.3.5


0.9.4
-----------------
* Update CLI `init` flow to support snowflake transient tables
* Use filename for default expectation suite name in CLI `init`
* Tables created by SqlAlchemyDataset use a shorter name with 8 hex characters of randomness instead of a full uuid
* Better error message when config substitution variable is missing
* removed an unused directory in the GE folder
* removed obsolete config error handling
* Docs typo fixes
* Jupyter notebook improvements
* `great_expectations init` improvements
* Simpler messaging in validation notebooks
* replaced hacky loop with suite list call in notebooks
* CLI suite new now supports `--empty` flag that generates an empty suite and opens a notebook
* add error handling to `init` flow for cases where user tries using a broken file


0.9.3
-----------------
* Add support for transient table creation in snowflake (#1012)
* Improve path support in TupleStoreBackend for better cross-platform compatibility
* New features on `ExpectationSuite`

  - ``add_citation()``
  - ``get_citations()``

* `SampleExpectationsDatasetProfiler` now leaves a citation containing the original batch kwargs
* `great_expectations suite edit` now uses batch_kwargs from citations if they exist
* Bugfix :: suite edit notebooks no longer blow away the existing suite while loading a batch of data
* More robust and tested logic in `suite edit`
* DataDocs: bugfixes and improvements for smaller viewports
* Bugfix :: fix for bug that crashes SampleExpectationsDatasetProfiler if unexpected_percent is of type decimal.Decimal (`#1109 <https://github.com/great-expectations/great_expectations/issues/1109>`_)


0.9.2
-----------------
* Fixes #1095
* Added a `list_expectation_suites` function to `data_context`, and a corresponding CLI function - `suite list`.
* CI no longer enforces legacy python tests.

0.9.1
------
* Bugfix for dynamic "How to Edit This Expectation Suite" command in DataDocs

0.9.0
-----------------

Version 0.9.0 is a major update to Great Expectations! The DataContext has continued to evolve into a powerful tool
for ensuring that Expectation Suites can properly represent the way users think about their data, and upgrading will
make it much easier to store and share expectation suites, and to build data docs that support your whole team.
You’ll get awesome new features including improvements to data docs look and the ability to choose and store metrics
for building flexible data quality dashboards.

The changes for version 0.9.0 fall into several broad areas:

1. Onboarding

Release 0.9.0 of Great Expectations makes it much easier to get started with the project. The `init` flow has grown
to support a much wider array of use cases and to use more natural language rather than introducing
GreatExpectations concepts earlier. You can more easily configure different backends and datasources, take advantage
of guided walkthroughs to find and profile data, and share project configurations with colleagues.

If you have already completed the `init` flow using a previous version of Great Expectations, you do not need to
rerun the command. However, **there are some small changes to your configuration that will be required**. See
:ref:`migrating_versions` for details.

2. CLI Command Improvements

With this release we have introduced a consistent naming pattern for accessing subcommands based on the noun (a
Great Expectations object like `suite` or `docs`) and verb (an action like `edit` or `new`). The new user experience
will allow us to more naturally organize access to CLI tools as new functionality is added.

3. Expectation Suite Naming and Namespace Changes

Defining shared expectation suites and validating data from different sources is much easier in this release. The
DataContext, which manages storage and configuration of expectations, validations, profiling, and data docs, no
longer requires that expectation suites live in a datasource-specific “namespace.” Instead, you should name suites
with the logical name corresponding to your data, making it easy to share them or validate against different data
sources. For example, the expectation suite "npi" for National Provider Identifier data can now be shared across
teams who access the same logical data in local systems using Pandas, on a distributed Spark cluster, or via a
relational database.

Batch Kwargs, or instructions for a datasource to build a batch of data, are similarly freed from a required
namespace, and you can more easily integrate Great Expectations into workflows where you do not need to use a
BatchKwargsGenerator (usually because you have a batch of data ready to validate, such as in a table or a known
directory).

The most noticeable impact of this API change is in the complete removal of the DataAssetIdentifier class. For
example, the `create_expectation_suite` and `get_batch` methods now no longer require a data_asset_name parameter,
relying only on the expectation_suite_name and batch_kwargs to do their job. Similarly, there is no more asset name
normalization required. See the upgrade guide for more information.

4. Metrics and Evaluation Parameter Stores

Metrics have received much more love in this release of Great Expectations! We've improved the system for declaring
evaluation parameters that support dependencies between different expectation suites, so you can easily identify a
particular field in the result of one expectation to use as the input into another. And the MetricsStore is now much
more flexible, supporting a new ValidationAction that makes it possible to select metrics from a validation result
to be saved in a database where they can power a dashboard.

5. Internal Type Changes and Improvements

Finally, in this release, we have done a lot of work under the hood to make things more robust, including updating
all of the internal objects to be more strongly typed. That change, while largely invisible to end users, paves the
way for some really exciting opportunities for extending Great Expectations as we build a bigger community around
the project.


We are really excited about this release, and encourage you to upgrade right away to take advantage of the more
flexible naming and simpler API for creating, accessing, and sharing your expectations. As always feel free to join
us on Slack for questions you don't see addressed!


0.8.9__develop
-----------------


0.8.8
-----------------
* Add support for allow_relative_error to expect_column_quantile_values_to_be_between, allowing Redshift users access
  to this expectation
* Add support for checking backend type information for datetime columns using expect_column_min_to_be_between and
  expect_column_max_to_be_between

0.8.7
-----------------
* Add support for expect_column_values_to_be_of_type for BigQuery backend (#940)
* Add image CDN for community usage stats
* Documentation improvements and fixes

0.8.6
-----------------
* Raise informative error if config variables are declared but unavailable
* Update ExpectationsStore defaults to be consistent across all FixedLengthTupleStoreBackend objects
* Add support for setting spark_options via SparkDFDatasource
* Include tail_weights by default when using build_continuous_partition_object
* Fix Redshift quantiles computation and type detection
* Allow boto3 options to be configured (#887)

0.8.5
-----------------
* BREAKING CHANGE: move all reader options from the top-level batch_kwargs object to a sub-dictionary called
  "reader_options" for SparkDFDatasource and PandasDatasource. This means it is no longer possible to specify
  supplemental reader-specific options at the top-level of `get_batch`,  `yield_batch_kwargs` or `build_batch_kwargs`
  calls, and instead, you must explicitly specify that they are reader_options, e.g. by a call such as:
  `context.yield_batch_kwargs(data_asset_name, reader_options={'encoding': 'utf-8'})`.
* BREAKING CHANGE: move all query_params from the top-level batch_kwargs object to a sub-dictionary called
  "query_params" for SqlAlchemyDatasource. This means it is no longer possible to specify supplemental query_params at
  the top-level of `get_batch`,  `yield_batch_kwargs` or `build_batch_kwargs`
  calls, and instead, you must explicitly specify that they are query_params, e.g. by a call such as:
  `context.yield_batch_kwargs(data_asset_name, query_params={'schema': 'foo'})`.
* Add support for filtering validation result suites and validation result pages to show only failed expectations in
  generated documentation
* Add support for limit parameter to batch_kwargs for all datasources: Pandas, SqlAlchemy, and SparkDF; add support
  to generators to support building batch_kwargs with limits specified.
* Include raw_query and query_params in query_generator batch_kwargs
* Rename generator keyword arguments from data_asset_name to generator_asset to avoid ambiguity with normalized names
* Consistently migrate timestamp from batch_kwargs to batch_id
* Include batch_id in validation results
* Fix issue where batch_id was not included in some generated datasets
* Fix rendering issue with expect_table_columns_to_match_ordered_list expectation
* Add support for GCP, including BigQuery and GCS
* Add support to S3 generator for retrieving directories by specifying the `directory_assets` configuration
* Fix warning regarding implicit class_name during init flow
* Expose build_generator API publicly on datasources
* Allow configuration of known extensions and return more informative message when SubdirReaderBatchKwargsGenerator cannot find
  relevant files.
* Add support for allow_relative_error on internal dataset quantile functions, and add support for
  build_continuous_partition_object in Redshift
* Fix truncated scroll bars in value_counts graphs


0.8.4.post0
----------------
* Correct a packaging issue resulting in missing notebooks in tarball release; update docs to reflect new notebook
  locations.


0.8.4
-----------------
* Improved the tutorials that walk new users through the process of creating expectations and validating data
* Changed the flow of the init command - now it creates the scaffolding of the project and adds a datasource. After
  that users can choose their path.
* Added a component with links to useful tutorials to the index page of the Data Docs website
* Improved the UX of adding a SQL datasource in the CLI - now the CLI asks for specific credentials for Postgres,
  MySQL, Redshift and Snowflake, allows continuing debugging in the config file and has better error messages
* Added batch_kwargs information to DataDocs validation results
* Fix an issue affecting file stores on Windows


0.8.3
-----------------
* Fix a bug in data-docs' rendering of mostly parameter
* Correct wording for expect_column_proportion_of_unique_values_to_be_between
* Set charset and meta tags to avoid unicode decode error in some browser/backend configurations
* Improve formatting of empirical histograms in validation result data docs
* Add support for using environment variables in `config_variables_file_path`
* Documentation improvements and corrections


0.8.2.post0
------------
* Correct a packaging issue resulting in missing css files in tarball release


0.8.2
-----------------
* Add easier support for customizing data-docs css
* Use higher precision for rendering 'mostly' parameter in data-docs; add more consistent locale-based
  formatting in data-docs
* Fix an issue causing visual overlap of large numbers of validation results in build-docs index
* Documentation fixes (thanks @DanielOliver!) and improvements
* Minor CLI wording fixes
* Improved handling of MySql temporary tables
* Improved detection of older config versions


0.8.1
-----------------
* Fix an issue where version was reported as '0+unknown'


0.8.0
-----------------

Version 0.8.0 is a significant update to Great Expectations, with many improvements focused on configurability
and usability.  See the :ref:`migrating_versions` guide for more details on specific changes, which include
several breaking changes to configs and APIs.

Highlights include:

1. Validation Operators and Actions. Validation operators make it easy to integrate GE into a variety of pipeline runners. They
   offer one-line integration that emphasizes configurability. See the :ref:`validation_operators_and_actions`
   feature guide for more information.

   - The DataContext `get_batch` method no longer treats `expectation_suite_name` or `batch_kwargs` as optional; they
     must be explicitly specified.
   - The top-level GE validate method allows more options for specifying the specific data_asset class to use.

2. First-class support for plugins in a DataContext, with several features that make it easier to configure and
   maintain DataContexts across common deployment patterns.

   - **Environments**: A DataContext can now manage :ref:`environment_and_secrets` more easily thanks to more dynamic and
     flexible variable substitution.
   - **Stores**: A new internal abstraction for DataContexts, :ref:`stores_reference`, make extending GE easier by
     consolidating logic for reading and writing resources from a database, local, or cloud storage.
   - **Types**: Utilities configured in a DataContext are now referenced using `class_name` and `module_name` throughout
     the DataContext configuration, making it easier to extend or supplement pre-built resources. For now, the "type"
     parameter is still supported but expect it to be removed in a future release.

3. Partitioners: Batch Kwargs are clarified and enhanced to help easily reference well-known chunks of data using a
   partition_id. Batch ID and Batch Fingerprint help round out support for enhanced metadata around data
   assets that GE validates. See :ref:`batch_identifiers` for more information. The `GlobReaderBatchKwargsGenerator`,
   `QueryBatchKwargsGenerator`, `S3GlobReaderBatchKwargsGenerator`, `SubdirReaderBatchKwargsGenerator`, and `TableBatchKwargsGenerator` all support partition_id for
   easily accessing data assets.

4. Other Improvements:

   - We're beginning a long process of some under-the-covers refactors designed to make GE more maintainable as we
     begin adding additional features.
   - Restructured documentation: our docs have a new structure and have been reorganized to provide space for more
     easily adding and accessing reference material. Stay tuned for additional detail.
   - The command build-documentation has been renamed build-docs and now by
     default opens the Data Docs in the users' browser.

v0.7.11
-----------------
* Fix an issue where head() lost the column name for SqlAlchemyDataset objects with a single column
* Fix logic for the 'auto' bin selection of `build_continuous_partition_object`
* Add missing jinja2 dependency
* Fix an issue with inconsistent availability of strict_min and strict_max options on expect_column_values_to_be_between
* Fix an issue where expectation suite evaluation_parameters could be overriden by values during validate operation


v0.7.10
-----------------
* Fix an issue in generated documentation where the Home button failed to return to the index
* Add S3 Generator to module docs and improve module docs formatting
* Add support for views to QueryBatchKwargsGenerator
* Add success/failure icons to index page
* Return to uniform histogram creation during profiling to avoid large partitions for internal performance reasons


v0.7.9
-----------------
* Add an S3 generator, which will introspect a configured bucket and generate batch_kwargs from identified objects
* Add support to PandasDatasource and SparkDFDatasource for reading directly from S3
* Enhance the Site Index page in documentation so that validation results are sorted and display the newest items first
  when using the default run-id scheme
* Add a new utility method, `build_continuous_partition_object` which will build partition objects using the dataset
  API and so supports any GE backend.
* Fix an issue where columns with spaces in their names caused failures in some SqlAlchemyDataset and SparkDFDataset
  expectations
* Fix an issue where generated queries including null checks failed on MSSQL (#695)
* Fix an issue where evaluation parameters passed in as a set instead of a list could cause JSON serialization problems
  for the result object (#699)


v0.7.8
-----------------
* BREAKING: slack webhook URL now must be in the profiles.yml file (treat as a secret)
* Profiler improvements:

  - Display candidate profiling data assets in alphabetical order
  - Add columns to the expectation_suite meta during profiling to support human-readable description information

* Improve handling of optional dependencies during CLI init
* Improve documentation for create_expectations notebook
* Fix several anachronistic documentation and docstring phrases (#659, #660, #668, #681; #thanks @StevenMMortimer)
* Fix data docs rendering issues:

  - documentation rendering failure from unrecognized profiled column type (#679; thanks @dinedal))
  - PY2 failure on encountering unicode (#676)


0.7.7
-----------------
* Standardize the way that plugin module loading works. DataContext will begin to use the new-style class and plugin
  identification moving forward; yml configs should specify class_name and module_name (with module_name optional for
  GE types). For now, it is possible to use the "type" parameter in configuration (as before).
* Add support for custom data_asset_type to all datasources
* Add support for strict_min and strict_max to inequality-based expectations to allow strict inequality checks
  (thanks @RoyalTS!)
* Add support for reader_method = "delta" to SparkDFDatasource
* Fix databricks generator (thanks @sspitz3!)
* Improve performance of DataContext loading by moving optional import
* Fix several memory and performance issues in SparkDFDataset.

  - Use only distinct value count instead of bringing values to driver
  - Migrate away from UDF for set membership, nullity, and regex expectations

* Fix several UI issues in the data_documentation

  - Move prescriptive dataset expectations to Overview section
  - Fix broken link on Home breadcrumb
  - Scroll follows navigation properly
  - Improved flow for long items in value_set
  - Improved testing for ValidationRenderer
  - Clarify dependencies introduced in documentation sites
  - Improve testing and documentation for site_builder, including run_id filter
  - Fix missing header in Index page and cut-off tooltip
  - Add run_id to path for validation files


0.7.6
-----------------
* New Validation Renderer! Supports turning validation results into HTML and displays differences between the expected
  and the observed attributes of a dataset.
* Data Documentation sites are now fully configurable; a data context can be configured to generate multiple
  sites built with different GE objects to support a variety of data documentation use cases. See data documentation
  guide for more detail.
* CLI now has a new top-level command, `build-documentation` that can support rendering documentation for specified
  sites and even named data assets in a specific site.
* Introduced DotDict and LooselyTypedDotDict classes that allow to enforce typing of dictionaries.
* Bug fixes: improved internal logic of rendering data documentation, slack notification, and CLI profile command when
  datasource argument was not provided.

0.7.5
-----------------
* Fix missing requirement for pypandoc brought in from markdown support for notes rendering.

0.7.4
-----------------
* Fix numerous rendering bugs and formatting issues for rendering documentation.
* Add support for pandas extension dtypes in pandas backend of expect_column_values_to_be_of_type and
  expect_column_values_to_be_in_type_list and fix bug affecting some dtype-based checks.
* Add datetime and boolean column-type detection in BasicDatasetProfiler.
* Improve BasicDatasetProfiler performance by disabling interactive evaluation when output of expectation is not
  immediately used for determining next expectations in profile.
* Add support for rendering expectation_suite and expectation_level notes from meta in docs.
* Fix minor formatting issue in readthedocs documentation.

0.7.3
-----------------
* BREAKING: Harmonize expect_column_values_to_be_of_type and expect_column_values_to_be_in_type_list semantics in
  Pandas with other backends, including support for None type and type_list parameters to support profiling.
  *These type expectations now rely exclusively on native python or numpy type names.*
* Add configurable support for Custom DataAsset modules to DataContext
* Improve support for setting and inheriting custom data_asset_type names
* Add tooltips with expectations backing data elements to rendered documentation
* Allow better selective disabling of tests (thanks @RoyalITS)
* Fix documentation build errors causing missing code blocks on readthedocs
* Update the parameter naming system in DataContext to reflect data_asset_name *and* expectation_suite_name
* Change scary warning about discarding expectations to be clearer, less scary, and only in log
* Improve profiler support for boolean types, value_counts, and type detection
* Allow user to specify data_assets to profile via CLI
* Support CLI rendering of expectation_suite and EVR-based documentation

0.7.2
-----------------
* Improved error detection and handling in CLI "add datasource" feature
* Fixes in rendering of profiling results (descriptive renderer of validation results)
* Query Generator of SQLAlchemy datasource adds tables in non-default schemas to the data asset namespace
* Added convenience methods to display HTML renderers of sections in Jupyter notebooks
* Implemented prescriptive rendering of expectations for most expectation types

0.7.1
------------

* Added documentation/tutorials/videos for onboarding and new profiling and documentation features
* Added prescriptive documentation built from expectation suites
* Improved index, layout, and navigation of data context HTML documentation site
* Bug fix: non-Python files were not included in the package
* Improved the rendering logic to gracefully deal with failed expectations
* Improved the basic dataset profiler to be more resilient
* Implement expect_column_values_to_be_of_type, expect_column_values_to_be_in_type_list for SparkDFDataset
* Updated CLI with a new documentation command and improved profile and render commands
* Expectation suites and validation results within a data context are saved in a more readable form (with indentation)
* Improved compatibility between SparkDatasource and InMemoryGenerator
* Optimization for Pandas column type checking
* Optimization for Spark duplicate value expectation (thanks @orenovadia!)
* Default run_id format no longer includes ":" and specifies UTC time
* Other internal improvements and bug fixes


0.7.0
------------

Version 0.7 of Great Expectations is HUGE. It introduces several major new features
and a large number of improvements, including breaking API changes.

The core vocabulary of expectations remains consistent. Upgrading to
the new version of GE will primarily require changes to code that
uses data contexts; existing expectation suites will require only changes
to top-level names.

 * Major update of Data Contexts. Data Contexts now offer significantly \
   more support for building and maintaining expectation suites and \
   interacting with existing pipeline systems, including providing a namespace for objects.\
   They can handle integrating, registering, and storing validation results, and
   provide a namespace for data assets, making **batches** first-class citizens in GE.
   Read more: :ref:`data_context` or :py:mod:`great_expectations.data_context`

 * Major refactor of autoinspect. Autoinspect is now built around a module
   called "profile" which provides a class-based structure for building
   expectation suites. There is no longer a default  "autoinspect_func" --
   calling autoinspect requires explicitly passing the desired profiler. See :ref:`profiling`

 * New "Compile to Docs" feature produces beautiful documentation from expectations and expectation
   validation reports, helping keep teams on the same page.

 * Name clarifications: we've stopped using the overloaded terms "expectations
   config" and "config" and instead use "expectation suite" to refer to a
   collection (or suite!) of expectations that can be used for validating a
   data asset.

   - Expectation Suites include several top level keys that are useful \
     for organizing content in a data context: data_asset_name, \
     expectation_suite_name, and data_asset_type. When a data_asset is \
     validated, those keys will be placed in the `meta` key of the \
     validation result.

 * Major enhancement to the CLI tool including `init`, `render` and more flexibility with `validate`

 * Added helper notebooks to make it easy to get started. Each notebook acts as a combination of \
   tutorial and code scaffolding, to help you quickly learn best practices by applying them to \
   your own data.

 * Relaxed constraints on expectation parameter values, making it possible to declare many column
   aggregate expectations in a way that is always "vacuously" true, such as
   ``expect_column_values_to_be_between`` ``None`` and ``None``. This makes it possible to progressively
   tighten expectations while using them as the basis for profiling results and documentation.

  * Enabled caching on dataset objects by default.

 * Bugfixes and improvements:

   * New expectations:

     * expect_column_quantile_values_to_be_between
     * expect_column_distinct_values_to_be_in_set

   * Added support for ``head`` method on all current backends, returning a PandasDataset
   * More implemented expectations for SparkDF Dataset with optimizations

     * expect_column_values_to_be_between
     * expect_column_median_to_be_between
     * expect_column_value_lengths_to_be_between

   * Optimized histogram fetching for SqlalchemyDataset and SparkDFDataset
   * Added cross-platform internal partition method, paving path for improved profiling
   * Fixed bug with outputstrftime not being honored in PandasDataset
   * Fixed series naming for column value counts
   * Standardized naming for expect_column_values_to_be_of_type
   * Standardized and made explicit use of sample normalization in stdev calculation
   * Added from_dataset helper
   * Internal testing improvements
   * Documentation reorganization and improvements
   * Introduce custom exceptions for more detailed error logs

0.6.1
------------
* Re-add testing (and support) for py2
* NOTE: Support for SqlAlchemyDataset and SparkDFDataset is enabled via optional install \
  (e.g. ``pip install great_expectations[sqlalchemy]`` or ``pip install great_expectations[spark]``)

0.6.0
------------
* Add support for SparkDFDataset and caching (HUGE work from @cselig)
* Migrate distributional expectations to new testing framework
* Add support for two new expectations: expect_column_distinct_values_to_contain_set
  and expect_column_distinct_values_to_equal_set (thanks @RoyalTS)
* FUTURE BREAKING CHANGE: The new cache mechanism for Datasets, \
  when enabled, causes GE to assume that dataset does not change between evaluation of individual expectations. \
  We anticipate this will become the future default behavior.
* BREAKING CHANGE: Drop official support pandas < 0.22

0.5.1
---------------
* **Fix** issue where no result_format available for expect_column_values_to_be_null caused error
* Use vectorized computation in pandas (#443, #445; thanks @RoyalTS)


0.5.0
----------------
* Restructured class hierarchy to have a more generic DataAsset parent that maintains expectation logic separate \
  from the tabular organization of Dataset expectations
* Added new FileDataAsset and associated expectations (#416 thanks @anhollis)
* Added support for date/datetime type columns in some SQLAlchemy expectations (#413)
* Added support for a multicolumn expectation, expect multicolumn values to be unique (#408)
* **Optimization**: You can now disable `partial_unexpected_counts` by setting the `partial_unexpected_count` value to \
  0 in the result_format argument, and we do not compute it when it would not be returned. (#431, thanks @eugmandel)
* **Fix**: Correct error in unexpected_percent computations for sqlalchemy when unexpected values exceed limit (#424)
* **Fix**: Pass meta object to expectation result (#415, thanks @jseeman)
* Add support for multicolumn expectations, with `expect_multicolumn_values_to_be_unique` as an example (#406)
* Add dataset class to from_pandas to simplify using custom datasets (#404, thanks @jtilly)
* Add schema support for sqlalchemy data context (#410, thanks @rahulj51)
* Minor documentation, warning, and testing improvements (thanks @zdog).


0.4.5
----------------
* Add a new autoinspect API and remove default expectations.
* Improve details for expect_table_columns_to_match_ordered_list (#379, thanks @rlshuhart)
* Linting fixes (thanks @elsander)
* Add support for dataset_class in from_pandas (thanks @jtilly)
* Improve redshift compatibility by correcting faulty isnull operator (thanks @avanderm)
* Adjust partitions to use tail_weight to improve JSON compatibility and
  support special cases of KL Divergence (thanks @anhollis)
* Enable custom_sql datasets for databases with multiple schemas, by
  adding a fallback for column reflection (#387, thanks @elsander)
* Remove `IF NOT EXISTS` check for custom sql temporary tables, for
  Redshift compatibility (#372, thanks @elsander)
* Allow users to pass args/kwargs for engine creation in
  SqlAlchemyDataContext (#369, thanks @elsander)
* Add support for custom schema in SqlAlchemyDataset (#370, thanks @elsander)
* Use getfullargspec to avoid deprecation warnings.
* Add expect_column_values_to_be_unique to SqlAlchemyDataset
* **Fix** map expectations for categorical columns (thanks @eugmandel)
* Improve internal testing suite (thanks @anhollis and @ccnobbli)
* Consistently use value_set instead of mixing value_set and values_set (thanks @njsmith8)

0.4.4
----------------
* Improve CLI help and set CLI return value to the number of unmet expectations
* Add error handling for empty columns to SqlAlchemyDataset, and associated tests
* **Fix** broken support for older pandas versions (#346)
* **Fix** pandas deepcopy issue (#342)

0.4.3
-------
* Improve type lists in expect_column_type_to_be[_in_list] (thanks @smontanaro and @ccnobbli)
* Update cli to use entry_points for conda compatibility, and add version option to cli
* Remove extraneous development dependency to airflow
* Address SQlAlchemy warnings in median computation
* Improve glossary in documentation
* Add 'statistics' section to validation report with overall validation results (thanks @sotte)
* Add support for parameterized expectations
* Improve support for custom expectations with better error messages (thanks @syk0saje)
* Implement expect_column_value_lenghts_to_[be_between|equal] for SQAlchemy (thanks @ccnobbli)
* **Fix** PandasDataset subclasses to inherit child class

0.4.2
-------
* **Fix** bugs in expect_column_values_to_[not]_be_null: computing unexpected value percentages and handling all-null (thanks @ccnobbli)
* Support mysql use of Decimal type (thanks @bouke-nederstigt)
* Add new expectation expect_column_values_to_not_match_regex_list.

  * Change behavior of expect_column_values_to_match_regex_list to use python re.findall in PandasDataset, relaxing \
    matching of individuals expressions to allow matches anywhere in the string.

* **Fix** documentation errors and other small errors (thanks @roblim, @ccnobbli)

0.4.1
-------
* Correct inclusion of new data_context module in source distribution

0.4.0
-------
* Initial implementation of data context API and SqlAlchemyDataset including implementations of the following \
  expectations:

  * expect_column_to_exist
  * expect_table_row_count_to_be
  * expect_table_row_count_to_be_between
  * expect_column_values_to_not_be_null
  * expect_column_values_to_be_null
  * expect_column_values_to_be_in_set
  * expect_column_values_to_be_between
  * expect_column_mean_to_be
  * expect_column_min_to_be
  * expect_column_max_to_be
  * expect_column_sum_to_be
  * expect_column_unique_value_count_to_be_between
  * expect_column_proportion_of_unique_values_to_be_between

* Major refactor of output_format to new result_format parameter. See docs for full details:

  * exception_list and related uses of the term exception have been renamed to unexpected
  * Output formats are explicitly hierarchical now, with BOOLEAN_ONLY < BASIC < SUMMARY < COMPLETE. \
    All *column_aggregate_expectation* expectations now return element count and related information included at the \
    BASIC level or higher.

* New expectation available for parameterized distributions--\
  expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than (what a name! :) -- (thanks @ccnobbli)
* ge.from_pandas() utility (thanks @schrockn)
* Pandas operations on a PandasDataset now return another PandasDataset (thanks @dlwhite5)
* expect_column_to_exist now takes a column_index parameter to specify column order (thanks @louispotok)
* Top-level validate option (ge.validate())
* ge.read_json() helper (thanks @rjurney)
* Behind-the-scenes improvements to testing framework to ensure parity across data contexts.
* Documentation improvements, bug-fixes, and internal api improvements

0.3.2
-------
* Include requirements file in source dist to support conda

0.3.1
--------
* **Fix** infinite recursion error when building custom expectations
* Catch dateutil parsing overflow errors

0.2
-----
* Distributional expectations and associated helpers are improved and renamed to be more clear regarding the tests they apply
* Expectation decorators have been refactored significantly to streamline implementing expectations and support custom expectations
* API and examples for custom expectations are available
* New output formats are available for all expectations
* Significant improvements to test suite and compatibility
