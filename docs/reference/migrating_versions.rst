.. _migrating_versions:

###################################
Migrating Between Versions
###################################

While we are committed to keeping Great Expectations as stable as possible,
sometimes breaking changes are necessary to maintain our trajectory. This is
especially true as the library has evolved from just a data quality tool to a
more capable framework including data docs and profiling in addition to validation.

Great Expectations provides a warning when the currently-installed version is
different from the version stored in the expectation suite.

Since expectation semantics are usually consistent across versions, there is
little change required when upgrading great expectations, with some exceptions
noted here.

***************************************
Using the project check-config Command
***************************************

To facilitate this substantial config format change, starting with version 0.8.0
we introduced ``project check-config`` to sanity check your config files. From your
project directory, run:

.. code-block:: bash

    great_expectations project check-config

This can be used at any time and will grow more robust and helpful as our
internal config typing system improves.

You will most likely be prompted to install a new template. Rest assured that
your original yaml file will be archived automatically for you. Even so, it's
in your source control system already, right? ;-)

*************************
Upgrading to 0.9.x
*************************

In the 0.9.0 release, there are several changes to the DataContext API.

CONFIGURATION CHANGES:

- FixedLengthTupleXXXX stores are renamed to TupleXXXX stores; they no
  longer allow or require (or allow) a key_length to be specified, but they
  do allow `filepath_prefix` and/or `filepath_suffix` to be configured as an
  alternative to an the `filepath_template`.
- ExtractAndStoreEvaluationParamsAction is renamed to
  StoreEvaluationParametersAction; a new StoreMetricsAction is available as
  well to allow DataContext-configured metrics to be saved.
- The InMemoryEvaluationParameterStore is replaced with the
  EvaluationParameterStore; EvaluationParameterStore and MetricsStore can
  both be configured to use DatabaseStoreBackend instead of the
  InMemoryStoreBackend.
- The `type` key can no longer be used in place of class_name in
  configuration. Use `class_name` instead.
- BatchKwargsGenerators are more explicitly named; we avoid use of the term
  "Generator" because it is ambiguous. All existing BatchKwargsGenerators have
  been renamed by substituting "BatchKwargsGenerator" for "Generator"; for
  example GlobReaderGenerator is now GlobReaderBatchKwargsGenerator.
- ReaderMethod is no longer an enum; it is a string of the actual method to
  be invoked (e.g. `read_csv` for pandas). That change makes it easy to
  specify arbitrary reader_methods via batch_kwargs (including read_pickle),
  BUT existing configurations using enum-based reader_method in batch_kwargs
  will need to update their code. For example, a pandas datasource would use
  `reader_method: read_csv`` instead of `reader_method: csv`

CODE CHANGES:

- DataAssetName and name normalization have been completely eliminated, which
  causes several related changes to code using the DataContext.

  - data_asset_name is **no longer** a parameter in the
    create_expectation_suite, get_expectation_suite, or get_batch commands;
    expectation suite names exist in an independent namespace.
  - batch_kwargs alone now define the batch to be received, and the
    datasource name **must** be included in batch_kwargs as the "datasource"
    key.
  - **A generator name is therefore no longer required to get data or define
    an expectation suite.**
  - The BatchKwargsGenerators API has been simplified; `build_batch_kwargs`
    should be the entrypoint for all cases of using a generator to get
    batch_kwargs, including when explicitly specifying a partition, limiting
    the number of returned rows, accessing saved kwargs, or using any other
    BatchKwargsGenerator feature. BatchKwargsGenerators *must* be attached to
    a specific datasource to be instantiated.

- **Database store tables are not compatible** between versions and require a
  manual migration; the new default table names are: `ge_validations_store`,
  `ge_expectations_store`, `ge_metrics`, and `ge_evaluation_parameters`. The
  Validations Store uses a three-part compound primary key consisting of
  run_id, expectation_suite_name, and batch_identifier; Expectations Store
  uses the expectation_suite_name as its only key. Both Metrics and
  Evaluation Parameters stores use `run_id`, `expectation_suite_name`,
  `metric_id`, and `metric_kwargs_id` to form a compound primary key.
- The term "batch_fingerprint" is no longer used, and has been replaced with
  "batch_markers". It is a dictionary that, like batch_kwargs, can be used to
  construct an ID.
- `get_data_asset_name` and `save_data_asset_name` are removed.
- There are numerous under-the-scenes changes to the internal types used in
  GreatExpectations. These should be transparent to users.


*************************
Upgrading to 0.8.x
*************************

In the 0.8.0 release, our DataContext config format has changed dramatically to
enable new features including extensibility.

Some specific changes:

- New top-level keys:

  - `expectations_store_name`
  - `evaluation_parameter_store_name`
  - `validations_store_name`

- Deprecation of the `type` key for configuring objects (replaced by
  `class_name` (and `module_name` as well when ambiguous).
- Completely new `SiteBuilder` configuration. See :ref:`data_docs_reference`.

BREAKING:
 - **top-level `validate` has a new signature**, that offers a variety of different options for specifying the DataAsset
   class to use during validation, including `data_asset_class_name` / `data_asset_module_name` or `data_asset_class`
 - Internal class name changes between alpha versions:
   - InMemoryEvaluationParameterStore
   - ValidationsStore
   - ExpectationsStore
   - ActionListValidationOperator
 - Several modules are now refactored into different names including all datasources
 - InMemoryBatchKwargs use the key dataset instead of df to be more explicit


Pre-0.8.x configuration files ``great_expectations.yml`` are not compatible with 0.8.x. Run ``great_expectations project check-config`` - it will offer to create a new config file. The new config file will not have any customizations you made, so you will have to copy these from the old file.

If you run into any issues, please ask for help on `Slack <https://greatexpectations.io/slack>`__.

*************************
Upgrading to 0.7.x
*************************

In version 0.7, GE introduced several new features, and significantly changed the way DataContext objects work:

 - A :ref:`data_context` object manages access to expectation suites and other configuration in addition to data assets.
   It provides a flexible but opinionated structure for creating and storing configuration and expectations in version
   control.

 - When upgrading from prior versions, the new :ref:`datasource` objects provide the same functionality that compute-
   environment-specific data context objects provided before, but with significantly more flexibility.

 - The term "autoinspect" is no longer used directly, having been replaced by a much more flexible :ref:`profiling`
   feature.