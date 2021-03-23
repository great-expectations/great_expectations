.. _changelog:

#########
Changelog
#########

develop
-----------------
* [BUGFIX] Remove parentheses call at os.curdir in data_context.py #2566 (thanks @henriquejsfj)
* [FEATURE] Added support for references to secrets stores for AWS Secrets Manager, GCP Secret Manager and Azure Key Vault in `great_expectations.yml` project config file (Thanks @Cedric-Magnan!)
* [BUGFIX] Sorter Configuration Added to DataConnectorConfig and DataConnectorConfigSchema #2572
* [BUGFIX] Remove autosave of Checkpoints in test_yaml_config and store SimpleCheckpoint as Checkpoint #2549

0.13.14
-----------------
* [BUGFIX] Use temporary paths in tests #2545
* [FEATURE] Allow custom data_asset_name for in-memory dataframes #2494
* [ENHANCEMENT] Restore cli functionality for legacy checkpoints #2511
* [BUGFIX] Can not create Azure Backend with TupleAzureBlobStoreBackend #2513 (thanks @benoitLebreton-perso)
* [BUGFIX] force azure to set content_type='text/html' if the file is HTML #2539 (thanks @benoitLebreton-perso)
* [BUGFIX] Temporarily pin SqlAlchemy to < 1.4.0 in requirements-dev-sqlalchemy.txt #2547
* [DOCS] Fix documentation links generated within template #2542 (thanks @thejasraju)
* [MAINTENANCE] Remove deprecated automerge config #2492

0.13.13
-----------------
* [ENHANCEMENT] Improve support for median calculation in Athena (Thanks @kuhnen!) #2521
* [ENHANCEMENT] Update `suite scaffold` to work with the UserConfigurableProfiler #2519
* [MAINTENANCE] Add support for spark 3 based spark_config #2481

0.13.12
-----------------

* [FEATURE] Added EmailAction as a new Validation Action (Thanks @Cedric-Magnan!) #2479
* [ENHANCEMENT] CLI global options and checkpoint functionality for v3 api #2497
* [DOCS] Renamed the "old" and the "new" APIs to "V2 (Batch Kwargs) API" and "V3 (Batch Request) API" and added an article with recommendations for choosing between them

0.13.11
-----------------
* [FEATURE] Add "table.head" metric
* [FEATURE] Add support for BatchData as a core GE concept for all Execution Engines. #2395
 * NOTE: As part of our improvements to the underlying Batch API, we have refactored BatchSpec to be part of the "core" package in Great Expectations, consistent with its role coordinating communication about Batches between the Datasource and Execution Engine abstractions.
* [ENHANCEMENT] Explicit support for schema_name in the SqlAlchemyBatchData #2465. Issue #2340
* [ENHANCEMENT] Data docs can now be built skipping the index page using the python API #2224
* [ENHANCEMENT] Evaluation parameter runtime values rendering in data docs if arithmetic is present #2447. Issue #2215
* [ENHANCEMENT] When connecting to new Datasource, CLI prompt is consistent with rest of GE #2434
* [ENHANCEMENT] Adds basic test for bad s3 paths generated from regex #2427 (Thanks @lukedyer-peak!)
* [ENHANCEMENT] Updated UserConfigurableProfiler date parsing error handling #2459
* [ENHANCEMENT] Clarification of self_check error messages #2304
* [ENHANCEMENT] Allows gzipped files and other encodings to be read from S3 #2440 (Thanks @luke321321!)
* [BUGFIX] `expect_column_unique_value_count_to_be_between` renderer bug (duplicate "Distinct (%)") #2455. Issue #2423
* [BUGFIX] Fix S3 Test issue by pinning `moto` version < 2.0.0 #2470
* [BUGFIX] Check for datetime-parseable strings in validate_metric_value_between_configuration #2419. Issue #2340 (Thanks @victorwyee!)
* [BUGFIX] `expect_compound_columns_to_be_unique` ExpectationConfig added #2471 Issue #2464
* [BUGFIX] In basic profiler, handle date parsing and overflow exceptions separately #2431 (Thanks @peterdhansen!)
* [BUGFIX] Fix sqlalchemy column comparisons when comparison was done between different datatypes #2443 (Thanks @peterdhansen!)
* [BUGFIX] Fix divide by zero error in expect_compound_columns_to_be_unique #2454 (Thanks @jdimatteo!)
* [DOCS] added how-to guide for user configurable profiler #2452
* [DOCS] Linked videos and minor documentation addition #2388
* [DOCS] Modifying getting started tutorial content to work with 0.13.8+ #2418
* [DOCS] add case studies to header in docs #2430
* [MAINTENANCE] Updates to Azure pipeline configurations #2462
* [MAINTENANCE] Allowing the tests to run with Docker-in-Windows #2402 (Thanks @Patechoc!)
* [MAINTENANCE] Add support for automatically building expectations gallery metadata #2386


0.13.10
-----------------
* [ENHANCEMENT] Optimize tests #2421
* [ENHANCEMENT] Add docstring for _invert_regex_to_data_reference_template #2428
* [ENHANCEMENT] Added expectation to check if data is in alphabetical ordering #2407 (Thanks @sethdmay!)
* [BUGFIX] Fixed a broken docs link #2433
* [BUGFIX] Missing `markown_text.j2` jinja template #2422
* [BUGFIX] parse_strings_as_datetimes error with user_configurable_profiler #2429
* [BUGFIX] Update `suite edit` and `suite scaffold` notebook renderers to output functional validation cells #2432
* [DOCS] Update how_to_create_custom_expectations_for_pandas.rst #2426 (Thanks @henriquejsfj!)
* [DOCS] Correct regex escape for data connectors #2425 (Thanks @lukedyer-peak!)
* [CONTRIB] Expectation: Matches benfords law with 80 percent confidence interval test #2406 (Thanks @vinodkri1!)


0.13.9
-----------------
* [FEATURE] Add TupleAzureBlobStoreBackend (thanks @syahdeini) #1975
* [FEATURE] Add get_metrics interface to Modular Expectations Validator API
* [ENHANCEMENT] Add possibility to pass boto3 configuration to TupleS3StoreBackend (Thanks for #1691 to @mgorsk1!) #2371
* [ENHANCEMENT] Removed the logic that prints the "This configuration object was built using version..." warning when current version of Great Expectations is not the same as the one used to build the suite, since it was not actionable #2366
* [ENHANCEMENT] Update Validator with more informative error message
* [BUGFIX] Ensure that batch_spec_passthrough is handled correctly by properly refactoring build_batch_spec and _generate_batch_spec_parameters_from_batch_definition for all DataConnector classes
* [BUGFIX] Display correct unexpected_percent in DataDocs - corrects the result object from map expectations to return the same "unexpected_percent" as is used to evaluate success (excluding null values from the denominator). The old value is now returned in a key called "unexpected_percent_total" (thanks @mlondschien) #1875
* [BUGFIX] Add python=3.7 argument to conda env creation (thanks @scouvreur!) #2391
* [BUGFIX] Fix issue with temporary table creation in MySQL #2389
* [BUGFIX] Remove duplicate code in data_context.store.tuple_store_backend (Thanks @vanderGoes)
* [BUGFIX] Fix issue where WarningAndFailureExpectationSuitesValidationOperator failing when warning suite fails
* [DOCS] Update How to instantiate a Data Context on Databricks Spark cluster for 0.13+ #2379
* [DOCS] How to load a Pandas DataFrame as a Batch #2327
* [DOCS] Added annotations for Expectations not yet ported to the new Modular Expectations API.
* [DOCS] How to load a Spark DataFrame as a Batch #2385
* [MAINTENANCE] Add checkpoint store to store backend defaults #2378


0.13.8
-----------------
* [FEATURE] New implementation of Checkpoints that uses dedicated CheckpointStore (based on the new ConfigurationStore mechanism) #2311, #2338
* [BUGFIX] Fix issue causing incorrect identification of partially-implemented expectations as not abstract #2334
* [BUGFIX] DataContext with multiple DataSources no longer scans all configurations #2250


0.13.7
-----------------
* [BUGFIX] Fix Local variable 'temp_table_schema_name' might be referenced before assignment bug in sqlalchemy_dataset.py #2302
* [MAINTENANCE] Ensure compatibility with new pip resolver v20.3+ #2256
* [ENHANCEMENT] Improvements in the how-to guide, run_diagnostics method in Expectation base class and Expectation templates to support the new rapid "dev loop" of community-contributed Expectations. #2296
* [ENHANCEMENT] Improvements in the output of Expectations tests to make it more legible. #2296
* [DOCS] Clarification of the instructions for using conda in the "Setting Up Your Dev Environment" doc. #2306


0.13.6
-----------------
* [ENHANCEMENT] Skip checks when great_expectations package did not change #2287
* [ENHANCEMENT] A how-to guide, run_diagnostics method in Expectation base class and Expectation templates to support the new rapid "dev loop" of community-contributed Expectations. #2222
* [BUGFIX] Fix Local variable 'query_schema' might be referenced before assignment bug in sqlalchemy_dataset.py #2286 (Thanks @alessandrolacorte!)
* [BUGFIX] Use correct schema to fetch table and column metadata #2284 (Thanks @armaandhull!)
* [BUGFIX] Updated sqlalchemy_dataset to convert numeric metrics to json_serializable up front, avoiding an issue where expectations on data immediately fail due to the conversion to/from json. #2207


0.13.5
-----------------
* [FEATURE] Add MicrosoftTeamsNotificationAction (Thanks @Antoninj!)
* [FEATURE] New ``contrib`` package #2264
* [ENHANCEMENT] Data docs can now be built skipping the index page using the python API #2224
* [ENHANCEMENT] Speed up new suite creation flow when connecting to Databases. Issue #1670 (Thanks @armaandhull!)
* [ENHANCEMENT] Serialize PySpark DataFrame by converting to dictionary #2237
* [BUGFIX] Mask passwords in DataContext.list_datasources(). Issue #2184
* [BUGFIX] Skip escaping substitution variables in escape_all_config_variables #2243. Issue #2196 (Thanks @
varundunga!)
* [BUGFIX] Pandas extension guessing #2239 (Thanks @sbrugman!)
* [BUGFIX] Replace runtime batch_data DataFrame with string #2240
* [BUGFIX] Update Notebook Render Tests to Reflect Updated Python Packages #2262
* [DOCS] Updated the code of conduct to mention events #2278
* [DOCS] Update the diagram for batch metadata #2161
* [DOCS] Update metrics.rst #2257
* [MAINTENANCE] Different versions of Pandas react differently to corrupt XLS files. #2230
* [MAINTENANCE] remove the obsolete TODO comments #2229 (Thanks @beyondacm!)
* [MAINTENANCE] Update run_id to airflow_run_id for clarity. #2233


0.13.4
-----------------
* [FEATURE] Implement expect_column_values_to_not_match_regex_list in Spark (Thanks @mikaylaedwards!)
* [ENHANCEMENT] Improve support for quantile calculations in Snowflake
* [ENHANCEMENT] DataDocs show values of Evaluation Parameters #2165. Issue #2010
* [ENHANCEMENT] Work on requirements.txt #2052 (Thanks @shapiroj18!)
* [ENHANCEMENT] expect_table_row_count_to_equal_other_table #2133
* [ENHANCEMENT] Improved support for quantile calculations in Snowflake #2176
* [ENHANCEMENT] DataDocs show values of Evaluation Parameters #2165
* [BUGFIX] Add pagination to TupleS3StoreBackend.list_keys() #2169. Issue #2164
* [BUGFIX] Fixed black conflict, upgraded black, made import optional #2183
* [BUGFIX] Made improvements for the treatment of decimals for database backends for lossy conversion #2207
* [BUGFIX] Pass manually_initialize_store_backend_id to database store backends to mirror functionality of other backends. Issue #2181
* [BUGFIX] Make glob_directive more permissive in ConfiguredAssetFilesystemDataConnector #2197. Issue #2193
* [DOCS] Added link to Youtube video on in-code contexts #2177
* [DOCS] Docstrings for DataConnector and associated classes #2172
* [DOCS] Custom expectations improvement #2179
* [DOCS] Add a conda example to creating virtualenvs #2189
* [DOCS] Fix Airflow logo URL #2198 (Thanks @floscha!)
* [DOCS] Update explore_expectations_in_a_notebook.rst #2174
* [DOCS] Change to DOCS that describe Evaluation Parameters #2209
* [MAINTENANCE] Removed mentions of show_cta_footer and added deprecation notes in usage stats #2190. Issue #2120

0.13.3
-----------------
* [ENHANCEMENT] Updated the BigQuery Integration to create a view instead of a table (thanks @alessandrolacorte!) #2082.
* [ENHANCEMENT] Allow  database store backend to support specification of schema in credentials file
* [ENHANCEMENT] Add support for connection_string and url in configuring DatabaseStoreBackend, bringing parity to other SQL-based objects. In the rare case of user code that instantiates a DatabaseStoreBackend without using the Great Expectations config architecture, users should ensure they are providing kwargs to init, because the init signature order has changed.
* [ENHANCEMENT] Improved exception handling in the Slack notifications rendering logic
* [ENHANCEMENT] Uniform configuration support for both 0.13 and 0.12 versions of the Datasource class
* [ENHANCEMENT] A single `DataContext.get_batch()` method supports both 0.13 and 0.12 style call arguments
* [ENHANCEMENT] Initializing DataContext in-code is now available in both 0.13 and 0.12 versions
* [BUGFIX] Fixed a bug in the error printing logic in several exception handling blocks in the Data Docs rendering. This will make it easier for users to submit error messages in case of an error in rendering.
* [DOCS] Miscellaneous doc improvements
* [DOCS] Update cloud composer workflow to use GCSStoreBackendDefaults

0.13.2
-----------------
* [ENHANCEMENT] Support avro format in Spark datasource (thanks @ryanaustincarlson!) #2122
* [ENHANCEMENT] Made improvements to the backend for expect_column_quantile_values_to_be_between #2127
* [ENHANCEMENT] Robust Representation in Configuration of Both Legacy and New Datasource
* [ENHANCEMENT] Continuing 0.13 clean-up and improvements
* [BUGFIX] Fix spark configuration not getting passed to the SparkSession builder (thanks @EricSteg!) #2124
* [BUGFIX] Misc bugfixes and improvements to code & documentation for new in-code data context API #2118
* [BUGFIX] When Introspecting a database, sql_data_connector will ignore view_names that are also system_tables
* [BUGFIX] Made improvements for code & documentation for in-code data context
* [BUGFIX] Fixed bug where TSQL mean on `int` columns returned incorrect result
* [DOCS] Updated explanation for ConfiguredAssetDataConnector and InferredAssetDataConnector
* [DOCS] General 0.13 docs improvements

0.13.1
-----------------
* [ENHANCEMENT] Improved data docs performance by ~30x for large projects and ~4x for smaller projects by changing instantiation of Jinja environment #2100
* [ENHANCEMENT] Allow  database store backend to support specification of schema in credentials file #2058 (thanks @GTLangseth!)
* [ENHANCEMENT] More detailed information in Datasource.self_check() diagnostic (concerning ExecutionEngine objects)
* [ENHANCEMENT] Improve UI for in-code data contexts #2068
* [ENHANCEMENT] Add a store_backend_id property to StoreBackend #2030, #2075
* [ENHANCEMENT] Use an existing expectation_store.store_backend_id to initialize an in-code DataContext #2046, #2075
* [BUGFIX] Corrected handling of boto3_options by PandasExecutionEngine
* [BUGFIX] New Expectation via CLI / SQL Query no longer throws TypeError
* [BUGFIX] Implement validator.default_expectations_arguments
* [DOCS] Fix doc create and editing expectations #2105 (thanks @Lee-W!)
* [DOCS] Updated documentation on 0.13 classes
* [DOCS] Fixed a typo in the HOWTO guide for adding a self-managed Spark datasource
* [DOCS] Updated documentation for new UI for in-code data contexts

0.13.0
-----------------
* INTRODUCING THE NEW MODULAR EXPECTATIONS API (Experimental): this release introduces a new way to create expectation logic in its own class, making it much easier to author and share expectations. ``Expectation`` and ``MetricProvider`` classes now work together to validate data and consolidate logic for all backends by function. See the how-to guides in our documentation for more information on how to use the new API.
* INTRODUCING THE NEW DATASOURCE API (Experimental): this release introduces a new way to connect to datasources providing much richer guarantees for discovering ("inferring") data assets and partitions. The new API replaces "BatchKwargs" and "BatchKwargsGenerators" with BatchDefinition and BatchSpec objects built from DataConnector classes. You can read about the new API in our docs.
* The Core Concepts section of our documentation has been updated with descriptions of the classes and concepts used in the new API; we will continue to update that section and welcome questions and improvements.
* BREAKING: Data Docs rendering is now handled in the new Modular Expectations, which means that any custom expectation rendering needs to be migrated to the new API to function in version 0.13.0.
* BREAKING: **Renamed** Datasource to LegacyDatasource and introduced the new Datasource class. Because most installations rely on one PandasDatasource, SqlAlchemyDatasource, or SparkDFDatasource, most users will not be affected. However, if you have implemented highly customized Datasource class inheriting from the base class, you may need to update your inheritance.
* BREAKING: The new Modular Expectations API will begin removing the ``parse_strings_as_datetimes`` and ``allow_cross_type_comparisons`` flags in expectations. Expectation Suites that use the flags will need to be updated to use the new Modular Expectations. In general, simply removing the flag will produce correct behavior; if you still want the exact same semantics, you should ensure your raw data already has typed datetime objects.
* **NOTE:** Both the new Datasource API and the new Modular Expectations API are *experimental* and will change somewhat during the next several point releases. We are extremely excited for your feedback while we iterate rapidly, and continue to welcome new community contributions.

0.12.10
-----------------
* [BUGFIX] Update requirements.txt for ruamel.yaml to >=0.16 - #2048 (thanks @mmetzger!)
* [BUGFIX] Added option to return scalar instead of list from query store #2060
* [BUGFIX] Add missing markdown_content_block_container #2063
* [BUGFIX] Fixed a divided by zero error for checkpoints on empty expectation suites #2064
* [BUGFIX] Updated sort to correctly return partial unexpected results when expect_column_values_to_be_of_type has more than one unexpected type #2074
* [BUGFIX] Resolve Data Docs resource identifier issues to speed up UpdateDataDocs action #2078
* [DOCS] Updated contribution changelog location #2051 (thanks @shapiroj18!)
* [DOCS] Adding Airflow operator and Astrononomer deploy guides #2070
* [DOCS] Missing image link to bigquery logo #2071 (thanks @nelsonauner!)

0.12.9
-----------------
* [BUGFIX] Fixed the import of s3fs to use the optional import pattern - issue #2053
* [DOCS] Updated the title styling and added a Discuss comment article for the OpsgenieAlertAction how-to guide

0.12.8
-----------------
* [FEATURE] Add OpsgenieAlertAction #2012 (thanks @miike!)
* [FEATURE] Add S3SubdirReaderBatchKwargsGenerator #2001 (thanks @noklam)
* [ENHANCEMENT] Snowflake uses temp tables by default while still allowing transient tables
* [ENHANCEMENT] Enabled use of lowercase table and column names in GE with the `use_quoted_name` key in batch_kwargs #2023
* [BUGFIX] Basic suite builder profiler (suite scaffold) now skips excluded expectations #2037
* [BUGFIX] Off-by-one error in linking to static images #2036 (thanks @NimaVaziri!)
* [BUGFIX] Improve handling of pandas NA type issue #2029 PR #2039 (thanks @isichei!)
* [DOCS] Update Virtual Environment Example #2027 (thanks @shapiroj18!)
* [DOCS] Update implemented_expectations.rst (thanks @jdimatteo!)
* [DOCS] Update how_to_configure_a_pandas_s3_datasource.rst #2042 (thanks @CarstenFrommhold!)

0.12.7
-----------------
* [ENHANCEMENT] CLI supports s3a:// or gs:// paths for Pandas Datasources (issue #2006)
* [ENHANCEMENT] Escape $ characters in configuration, support multiple substitutions (#2005 & #2015)
* [ENHANCEMENT] Implement Skip prompt flag on datasource profile cli (#1881 Thanks @thcidale0808!)
* [BUGFIX] Fixed bug where slack messages cause stacktrace when data docs pages have issue
* [DOCS] How to use docker images (#1797)
* [DOCS] Remove incorrect doc line from PagerdutyAlertAction (Thanks @niallrees!)
* [MAINTENANCE] Update broken link (Thanks @noklam!)
* [MAINTENANCE] Fix path for how-to guide (Thanks @gauthamzz!)

0.12.6
-----------------
* [BUGFIX] replace black in requirements.txt

0.12.5
-----------------
* [ENHANCEMENT] Implement expect_column_values_to_be_json_parseable in spark (Thanks @mikaylaedwards!)
* [ENHANCEMENT] Fix boto3 options passing into datasource correctly (Thanks @noklam!)
* [ENHANCEMENT] Add .pkl to list of recognized extensions (Thanks @KPLauritzen!)
* [BUGFIX] Query batch kwargs support for Athena backend (issue 1964)
* [BUGFIX] Skip config substitution if key is "password" (issue 1927)
* [BUGFIX] fix site_names functionality and add site_names param to get_docs_sites_urls (issue 1991)
* [BUGFIX] Always render expectation suites in data docs unless passing a specific ExpectationSuiteIdentifier in resource_identifiers (issue 1944)
* [BUGFIX] remove black from requirements.txt
* [BUGFIX] docs build cli: fix --yes argument (Thanks @varunbpatil!)
* [DOCS] Update docstring for SubdirReaderBatchKwargsGenerator (Thanks @KPLauritzen!)
* [DOCS] Fix broken link in README.md (Thanks @eyaltrabelsi!)
* [DOCS] Clarifications on several docs (Thanks all!!)

0.12.4
-----------------
* [FEATURE] Add PagerdutyAlertAction (Thanks @NiallRees!)
* [FEATURE] enable using Minio for S3 backend (Thanks @noklam!)
* [ENHANCEMENT] Add SqlAlchemy support for expect_compound_columns_to_be_unique (Thanks @jhweaver!)
* [ENHANCEMENT] Add Spark support for expect_compound_columns_to_be_unique (Thanks @tscottcoombes1!)
* [ENHANCEMENT] Save expectation suites with datetimes in evaluation parameters (Thanks @mbakunze!)
* [ENHANCEMENT] Show data asset name in Slack message (Thanks @haydarai!)
* [ENHANCEMENT] Enhance data doc to show data asset name in overview block (Thanks @noklam!)
* [ENHANCEMENT] Clean up checkpoint output
* [BUGFIX] Change default prefix for TupleStoreBackend (issue 1907)
* [BUGFIX] Duplicate s3 approach for GCS for building object keys
* [BUGFIX] import NotebookConfig (Thanks @cclauss!)
* [BUGFIX] Improve links (Thanks @sbrugman!)
* [MAINTENANCE] Unpin black in requirements (Thanks @jtilly!)
* [MAINTENANCE] remove test case name special characters

0.12.3
-----------------
* [ENHANCEMENT] Add expect_compound_columns_to_be_unique and clarify multicolumn uniqueness
* [ENHANCEMENT] Add expectation expect_table_columns_to_match_set
* [ENHANCEMENT] Checkpoint run command now prints out details on each validation #1437
* [ENHANCEMENT] Slack notifications can now display links to GCS-hosted DataDocs sites
* [ENHANCEMENT] Public base URL can be configured for Data Docs sites
* [ENHANCEMENT] SuiteEditNotebookRenderer.add_header class now allows usage of env variables in jinja templates (thanks @mbakunze)!
* [ENHANCEMENT] Display table for Cramer's Phi expectation in Data Docs (thanks @mlondschien)!
* [BUGFIX] Explicitly convert keys to tuples when removing from TupleS3StoreBackend (thanks @balexander)!
* [BUGFIX] Use more-specific s3.meta.client.exceptions with dealing with boto resource api (thanks @lcorneliussen)!
* [BUGFIX] Links to Amazon S3 are compatible with virtual host-style access and path-style access
* [DOCS] How to Instantiate a Data Context on a Databricks Spark Cluster
* [DOCS] Update to Deploying Great Expectations with Google Cloud Composer
* [MAINTENANCE] Update moto dependency to include cryptography (see #spulec/moto/3290)

0.12.2
-----------------
* [ENHANCEMENT] Update schema for anonymized expectation types to avoid large key domain
* [ENHANCEMENT] BaseProfiler type mapping expanded to include more pandas and numpy dtypes
* [BUGFIX] Allow for pandas reader option inference with parquet and Excel (thanks @dlachasse)!
* [BUGFIX] Fix bug where running checkpoint fails if GCS data docs site has a prefix (thanks @sergii-tsymbal-exa)!
* [BUGFIX] Fix bug in deleting datasource config from config file (thanks @rxmeez)!
* [BUGFIX] clarify inclusiveness of min/max values in string rendering
* [BUGFIX] Building data docs no longer crashes when a data asset name is an integer #1913
* [DOCS] Add notes on transient table creation to Snowflake guide (thanks @verhey)!
* [DOCS] Fixed several broken links and glossary organization (thanks @JavierMonton and @sbrugman)!
* [DOCS] Deploying Great Expectations with Google Cloud Composer (Hosted Airflow)

0.12.1
-----------------
* [FEATURE] Add ``expect_column_pair_cramers_phi_value_to_be_less_than`` expectation to ``PandasDatasource`` to check for the independence of two columns by computing their Cramers Phi (thanks @mlondschien)!
* [FEATURE] add support for ``expect_column_pair_values_to_be_in_set`` to ``Spark`` (thanks @mikaylaedwards)!
* [FEATURE] Add new expectation:`` expect_multicolumn_sum_to_equal`` for ``pandas` and ``Spark`` (thanks @chipmyersjr)!
* [ENHANCEMENT] Update isort, pre-commit & pre-commit hooks, start more linting (thanks @dandandan)!
* [ENHANCEMENT] Bundle shaded marshmallow==3.7.1 to avoid dependency conflicts on GCP Composer
* [ENHANCEMENT] Improve row_condition support in aggregate expectations
* [BUGFIX] SuiteEditNotebookRenderer no longer break GCS and S3 data paths
* [BUGFIX] Fix bug preventing the use of get_available_partition_ids in s3 generator
* [BUGFIX] SuiteEditNotebookRenderer no longer break GCS and S3 data paths
* [BUGFIX] TupleGCSStoreBackend: remove duplicate prefix for urls (thanks @azban)!
* [BUGFIX] Fix `TypeError: unhashable type` error in Data Docs rendering

0.12.0
-----------------
* [BREAKING] This release includes a breaking change that *only* affects users who directly call `add_expectation`, `remove_expectation`, or `find_expectations`. (Most users do not use these APIs but add Expectations by stating them directly on Datasets). Those methods have been updated to take an ExpectationConfiguration object and `match_type` object. The change provides more flexibility in determining which expectations should be modified and allows us provide substantially improved support for two major features that we have frequently heard requested: conditional Expectations and more flexible multi-column custom expectations. See :ref:`expectation_suite_operations` and :ref:`migrating_versions` for more information.
* [FEATURE] Add support for conditional expectations using pandas execution engine (#1217 HUGE thanks @arsenii!)
* [FEATURE] ValidationActions can now consume and return "payload", which can be used to share information across ValidationActions
* [FEATURE] Add support for nested columns in the PySpark expectations (thanks @bramelfrink)!
* [FEATURE] add support for `expect_column_values_to_be_increasing` to `Spark` (thanks @mikaylaedwards)!
* [FEATURE] add support for `expect_column_values_to_be_decreasing` to `Spark` (thanks @mikaylaedwards)!
* [FEATURE] Slack Messages sent as ValidationActions now have link to DataDocs, if available.
* [FEATURE] Expectations now define “domain,” “success,” and “runtime” kwargs to allow them to determine expectation equivalence for updating expectations. Fixes column pair expectation update logic.
* [ENHANCEMENT] Add a `skip_and_clean_missing` flag to `DefaultSiteIndexBuilder.build` (default True). If True, when an index page is being built and an existing HTML page does not have corresponding source data (i.e. an expectation suite or validation result was removed from source store), the HTML page is automatically deleted and will not appear in the index. This ensures that the expectations store and validations store are the source of truth for Data Docs.
* [ENHANCEMENT] Include datetime and bool column types in descriptive documentation results
* [ENHANCEMENT] Improve data docs page breadcrumbs to have clearer run information
* [ENHANCEMENT] Data Docs Validation Results only shows unexpected value counts if all unexpected values are available
* [ENHANCEMENT] Convert GE version key from great_expectations.__version__ to great_expectations_version (thanks, @cwerner!) (#1606)
* [ENHANCEMENT] Add support in JSON Schema profiler for combining schema with anyOf key and creating nullability expectations
* [BUGFIX] Add guard for checking Redshift Dialect in match_like_pattern expectation
* [BUGFIX] Fix content_block build failure for dictionary content - (thanks @jliew!) #1722
* [BUGFIX] Fix bug that was preventing env var substitution in `config_variables.yml` when not at the top level
* [BUGFIX] Fix issue where expect_column_values_to_be_in_type_list did not work with positional type_list argument in SqlAlchemyDataset or SparkDFDataset
* [BUGFIX] Fixes a bug that was causing exceptions to occur if user had a Data Docs config excluding a particular site section
* [DOCS] Add how-to guides for configuring MySQL and MSSQL Datasources
* [DOCS] Add information about issue tags to contributing docs
* [DEPRECATION] Deprecate demo suite behavior in `suite new`

0.11.9
-----------------
* [FEATURE] New Dataset Support: Microsoft SQL Server
* [FEATURE] Render expectation validation results to markdown
* [FEATURE] Add --assume-yes/--yes/-y option to cli docs build command (thanks @feluelle)
* [FEATURE] Add SSO and SSH key pair authentication for Snowflake (thanks @dmateusp)
* [FEATURE] Add pattern-matching expectations that use the Standard SQL "LIKE" operator: "expect_column_values_to_match_like_pattern", "expect_column_values_to_not_match_like_pattern", "expect_column_values_to_match_like_pattern_list", and "expect_column_values_to_not_match_like_pattern_list"
* [ENHANCEMENT] Make Data Docs rendering of profiling results more flexible by deprecating the reliance on validation results having the specific run_name of "profiling"
* [ENHANCEMENT] Use green checkmark in Slack msgs instead of tada
* [ENHANCEMENT] log class instantiation errors for better debugging
* [BUGFIX] usage_statistics decorator now handles 'dry_run' flag
* [BUGFIX] Add spark_context to DatasourceConfigSchema (#1713) (thanks @Dandandan)
* [BUGFIX] Handle case when unexpected_count list element is str
* [DOCS] Deploying Data Docs
* [DOCS] New how-to guide: How to instantiate a Data Context on an EMR Spark cluster
* [DOCS] Managed Spark DF Documentation #1729 (thanks @mgorsk1)
* [DOCS] Typos and clarifications (thanks @dechoma @sbrugman @rexboyce)

0.11.8
-----------------
* [FEATURE] Customizable "Suite Edit" generated notebooks
* [ENHANCEMENT] Add support and docs for loading evaluation parameter from SQL database
* [ENHANCEMENT] Fixed some typos/grammar and a broken link in the suite_scaffold_notebook_renderer
* [ENHANCEMENT] allow updates to DatabaseStoreBackend keys by default, requiring `allow_update=False` to disallow
* [ENHANCEMENT] Improve support for prefixes declared in TupleS3StoreBackend that include reserved characters
* [BUGFIX] Fix issue where allow_updates was set for StoreBackend that did not support it
* [BUGFIX] Fix issue where GlobReaderBatchKwargsGenerator failed with relative base_directory
* [BUGFIX] Adding explicit requirement for "importlib-metadata" (needed for Python versions prior to Python 3.8).
* [MAINTENANCE] Install GitHub Dependabot
* [BUGFIX] Fix missing importlib for python 3.8 #1651

0.11.7
-----------------
* [ENHANCEMENT] Improve CLI error handling.
* [ENHANCEMENT] Do not register signal handlers if not running in main thread
* [ENHANCEMENT] store_backend (S3 and GCS) now throws InvalidKeyError if file does not exist at expected location
* [BUGFIX] ProfilerTypeMapping uses lists instead of sets to prevent serialization errors when saving suites created by JsonSchemaProfiler
* [DOCS] Update suite scaffold how-to
* [DOCS] Docs/how to define expectations that span multiple tables
* [DOCS] how to metadata stores validation on s3

0.11.6
-----------------
* [FEATURE] Auto-install Python DB packages.  If the required packages for a DB library are not installed, GE will offer the user to install them, without exiting CLI
* [FEATURE] Add new expectation expect_table_row_count_to_equal_other_table for SqlAlchemyDataset
* [FEATURE] A profiler that builds suites from JSONSchema files
* [ENHANCEMENT] Add ``.feather`` file support to PandasDatasource
* [ENHANCEMENT] Use ``colorama init`` to support terminal color on Windows
* [ENHANCEMENT] Update how_to_trigger_slack_notifications_as_a_validation_action.rst
* [ENHANCEMENT] Added note for config_version in great_expectations.yml
* [ENHANCEMENT] Implement "column_quantiles" for MySQL (via a compound SQLAlchemy query, since MySQL does not support "percentile_disc")
* [BUGFIX] "data_asset.validate" events with "data_asset_name" key in the batch kwargs were failing schema validation
* [BUGFIX] database_store_backend does not support storing Expectations in DB
* [BUGFIX] instantiation of ExpectationSuite always adds GE version metadata to prevent datadocs from crashing
* [BUGFIX] Fix all tests having to do with missing data source libraries
* [DOCS] will/docs/how_to/Store Expectations on Google Cloud Store

0.11.5
-----------------
* [FEATURE] Add support for expect_column_values_to_match_regex_list exception for Spark backend
* [ENHANCEMENT] Added 3 new usage stats events: "cli.new_ds_choice", "data_context.add_datasource", and "datasource.sqlalchemy.connect"
* [ENHANCEMENT] Support platform_specific_separator flag for TupleS3StoreBackend prefix
* [ENHANCEMENT] Allow environment substitution in config_variables.yml
* [BUGFIX] fixed issue where calling head() on a SqlAlchemyDataset would fail if the underlying table is empty
* [BUGFIX] fixed bug in rounding of mostly argument to nullity expectations produced by the BasicSuiteBuilderProfiler
* [DOCS] New How-to guide: How to add a Validation Operator (+ updated in Validation Operator doc strings)

0.11.4
-----------------
* [BUGIFX] Fixed an error that crashed the CLI when called in an environment with neither SQLAlchemy nor google.auth installed

0.11.3
-----------------
* [ENHANCEMENT] Removed the misleading scary "Site doesn't exist or is inaccessible" message that the CLI displayed before building Data Docs for the first time.
* [ENHANCEMENT] Catch sqlalchemy.exc.ArgumentError and google.auth.exceptions.GoogleAuthError in SqlAlchemyDatasource __init__ and re-raise them as DatasourceInitializationError - this allows the CLI to execute its retry logic when users provide a malformed SQLAlchemy URL or attempt to connect to a BigQuery project without having proper authentication.
* [BUGFIX] Fixed issue where the URL of the Glossary of Expectations article in the auto-generated suite edit notebook was wrong (out of date) (#1557).
* [BUGFIX] Use renderer_type to set paths in jinja templates instead of utm_medium since utm_medium is optional
* [ENHANCEMENT] Bring in custom_views_directory in DefaultJinjaView to enable custom jinja templates stored in plugins dir
* [BUGFIX] fixed glossary links in walkthrough modal, README, CTA button, scaffold notebook
* [BUGFIX] Improved TupleGCSStoreBackend configurability (#1398 #1399)
* [BUGFIX] Data Docs: switch bootstrap-table-filter-control.min.js to CDN
* [ENHANCEMENT] BasicSuiteBuilderProfiler now rounds mostly values for readability
* [DOCS] Add AutoAPI as the primary source for API Reference docs.

0.11.2
-----------------
* [FEATURE] Add support for expect_volumn_values_to_match_json_schema exception for Spark backend (thanks @chipmyersjr!)
* [ENHANCEMENT] Add formatted __repr__ for ValidationOperatorResult
* [ENHANCEMENT] add option to suppress logging when getting expectation suite
* [BUGFIX] Fix object name construction when calling SqlAlchemyDataset.head (thanks @mascah!)
* [BUGFIX] Fixed bug where evaluation parameters used in arithmetic expressions would not be identified as upstream dependencies.
* [BUGFIX] Fix issue where DatabaseStoreBackend threw IntegrityError when storing same metric twice
* [FEATURE] Added new cli upgrade helper to help facilitate upgrading projects to be compatible with GE 0.11.
  See :ref:`upgrading_to_0.11` for more info.
* [BUGFIX] Fixed bug preventing GCS Data Docs sites to cleaned
* [BUGFIX] Correct doc link in checkpoint yml
* [BUGFIX] Fixed issue where CLI checkpoint list truncated names (#1518)
* [BUGFIX] Fix S3 Batch Kwargs Generator incorrect migration to new build_batch_kwargs API
* [BUGFIX] Fix missing images in data docs walkthrough modal
* [BUGFIX] Fix bug in checkpoints that was causing incorrect run_time to be set
* [BUGFIX] Fix issue where data docs could remove trailing zeros from values when low precision was requested

0.11.1
-----------------
* [BUGFIX] Fixed bug that was caused by comparison between timezone aware and non-aware datetimes
* [DOCS] Updated docs with info on typed run ids and validation operator results
* [BUGFIX] Update call-to-action buttons on index page with correct URLs

0.11.0
-----------------
* [BREAKING] ``run_id`` is now typed using the new ``RunIdentifier`` class, which consists of a ``run_time`` and
  ``run_name``. Existing projects that have Expectation Suite Validation Results must be migrated.
  See :ref:`upgrading_to_0.11` for instructions.
* [BREAKING] ``ValidationMetric`` and ``ValidationMetricIdentifier`` objects now have a ``data_asset_name`` attribute.
  Existing projects with evaluation parameter stores that have database backends must be migrated.
  See :ref:`upgrading_to_0.11` for instructions.
* [BREAKING] ``ValidationOperator.run`` now returns an instance of new type, ``ValidationOperatorResult`` (instead of a
  dictionary). If your code uses output from Validation Operators, it must be updated.
* Major update to the styling and organization of documentation! Watch for more content and reorganization as we continue to improve the documentation experience with Great Expectations.
* [FEATURE] Data Docs: redesigned index page with paginated/sortable/searchable/filterable tables
* [FEATURE] Data Docs: searchable tables on Expectation Suite Validation Result pages
* ``data_asset_name`` is now added to batch_kwargs by batch_kwargs_generators (if available) and surfaced in Data Docs
* Renamed all ``generator_asset`` parameters to ``data_asset_name``
* Updated the dateutil dependency
* Added experimental QueryStore
* Removed deprecated cli tap command
* Added of 0.11 upgrade helper
* Corrected Scaffold maturity language in notebook to Experimental
* Updated the installation/configuration documentation for Snowflake users
* [ENHANCEMENT] Improved error messages for misconfigured checkpoints.
* [BUGFIX] Fixed bug that could cause some substituted variables in DataContext config to be saved to `great_expectations.yml`

0.10.12
-----------------
* [DOCS] Improved help for CLI `checkpoint` command
* [BUGFIX] BasicSuiteBuilderProfiler could include extra expectations when only some expectations were selected (#1422)
* [FEATURE] add support for `expect_multicolumn_values_to_be_unique` and `expect_column_pair_values_A_to_be_greater_than_B`
  to `Spark`. Thanks @WilliamWsyHK!
* [ENHANCEMENT] Allow a dictionary of variables can be passed to the DataContext constructor to allow override
  config variables at runtime. Thanks @balexander!
* [FEATURE] add support for `expect_column_pair_values_A_to_be_greater_than_B` to `Spark`.
* [BUGFIX] Remove SQLAlchemy typehints to avoid requiring library (thanks @mzjp2)!
* [BUGFIX] Fix issue where quantile boundaries could not be set to zero. Thanks @kokes!

0.10.11
-----------------
* Bugfix: build_data_docs list_keys for GCS returns keys and when empty a more user friendly message
* ENHANCEMENT: Enable Redshift Quantile Profiling


0.10.10
-----------------
* Removed out-of-date Airflow integration examples. This repo provides a comprehensive example of Airflow integration: `#GE Airflow Example <https://github.com/superconductive/ge_tutorials>`_
* Bugfix suite scaffold notebook now has correct suite name in first markdown cell.
* Bugfix: fixed an example in the custom expectations documentation article - "result" key was missing in the returned dictionary
* Data Docs Bugfix: template string substitution is now done using .safe_substitute(), to handle cases where string templates
  or substitution params have extraneous $ signs. Also added logic to handle templates where intended output has groupings of 2 or more $ signs
* Docs fix: fix in yml for example action_list_operator for metrics
* GE is now auto-linted using Black

-----------------

* DataContext.get_docs_sites_urls now raises error if non-existent site_name is specified
* Bugfix for the CLI command `docs build` ignoring the --site_name argument (#1378)
* Bugfix and refactor for `datasource delete` CLI command (#1386) @mzjp2
* Instantiate datasources and validate config only when datasource is used (#1374) @mzjp2
* suite delete changed from an optional argument to a required one
* bugfix for uploading objects to GCP #1393
* added a new usage stats event for the case when a data context is created through CLI
* tuplefilestore backend, expectationstore backend remove_key bugs fixed
* no url is returned on empty data_docs site
* return url for resource only if key exists
* Test added for the period special char case
* updated checkpoint module to not require sqlalchemy
* added BigQuery as an option in the list of databases in the CLI
* added special cases for handling BigQuery - table names are already qualified with schema name, so we must make sure that we do not prepend the schema name twice
* changed the prompt for the name of the temp table in BigQuery in the CLI to hint that a fully qualified name (project.dataset.table) should be provided
* Bugfix for: expect_column_quantile_values_to_be_between expectation throws an "unexpected keyword WITHIN" on BigQuery (#1391)

0.10.8
-----------------
* added support for overriding the default jupyter command via a GE_JUPYTER_COMMAND environment variable (#1347) @nehiljain
* Bugfix for checkpoint missing template (#1379)

0.10.7
-----------------
* crud delete suite bug fix

0.10.6
-----------------

* Checkpoints: a new feature to ease deployment of suites into your pipelines
  - DataContext.list_checkpoints() returns a list of checkpoint names found in the project
  - DataContext.get_checkpoint() returns a validated dictionary loaded from yml
  - new cli commands

    - `checkpoint new`
    - `checkpoint list`
    - `checkpoint run`
    - `checkpoint script`

* marked cli `tap` commands as deprecating on next release
* marked cli `validation-operator run` command as deprecating
* internal improvements in the cli code
* Improve UpdateDataDocsAction docs

0.10.5
-----------------

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
* Marked tap command for deprecation in next major release

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
* (BREAKING) Clarified API language: renamed all ``generator`` parameters and methods to the more correct ``batch_kwargs_generator`` language. Existing projects may require simple migration steps. See :ref:`Upgrading to 0.10.x <upgrading_to_0.10.x>` for instructions.
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
   - **Stores**: A new internal abstraction for DataContexts, :ref:`Stores <reference__core_concepts__data_context__stores>`, make extending GE easier by
     consolidating logic for reading and writing resources from a database, local, or cloud storage.
   - **Types**: Utilities configured in a DataContext are now referenced using `class_name` and `module_name` throughout
     the DataContext configuration, making it easier to extend or supplement pre-built resources. For now, the "type"
     parameter is still supported but expect it to be removed in a future release.

3. Partitioners: Batch Kwargs are clarified and enhanced to help easily reference well-known chunks of data using a
   partition_id. Batch ID and Batch Fingerprint help round out support for enhanced metadata around data
   assets that GE validates. See :ref:`Batch Identifiers <reference__core_concepts__batch_parameters>` for more information. The `GlobReaderBatchKwargsGenerator`,
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
