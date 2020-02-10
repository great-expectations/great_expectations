.. _migrating_versions:

###################################
Migrating Between Versions
###################################

While we are committed to keeping Great Expectations as stable as possible,
sometimes breaking changes are necessary to maintain our trajectory. This is
especially true as the library has evolved from just a data quality tool to a
slightly more opinionated framework.

Great Expectations provides a warning when the currently-installed version is
different from the version stored in the expectation suite.

Since expectation semantics are usually consistent across versions, there is
little change required when upgrading great expectations, with some exceptions
noted here.

*********************************
Using the project check-config Command
*********************************

To facilitate this substantial config format change, starting with version 0.8.0
we introduced ``project check-config`` to sanity check your config files. From your
project directory, run:

>>> great_expectations project check-config

This can be used at any time and will grow more robust and helpful as our
internal config typing system improves.

You will most likely be prompted to install a new template. Rest assured that
your original yaml file will be archived automatically for you. Even so, it's
in your source control system already, right? ;-)

*************************
Upgrading to 0.9.x
*************************

In the 0.9.0 release, there are several additional changes to the DataContext API.

BREAKING:
- FixedLengthTupleXXXX stores are renamed to TupleXXXX stores; they no longer allow or require a key_length to be
  specified.
- data_asset_name is not used as a parameter in the create_expectation_suite, get_expectation_suite, or get_batch
  commands. Instead, batch_kwargs alone now define the batch to be received, and expectation suite names exist in an
  independent namespace.
- "Generator" classes are now more explicitly named "BatchKwargsGenerator" classes; for example, the S3Generator is
  now the S3GlobReaderBatchKwargsGenerator

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