.. _changelog:

#########
Changelog
#########


develop
-----------------

0.14.8
-----------------
* [FEATURE] Add `run_profiler_on_data` method to DataContext (#4190)
* [FEATURE] `RegexPatternStringParameterBuilder` for `RuleBasedProfiler` (#4167)
* [FEATURE] experimental column map expectation checking for vectors (#3102) (thanks @manyshapes)
* [FEATURE] Pre-requisites in Rule-Based Profiler for Self-Estimating Expectations (#4242)
* [FEATURE] Add optional parameter `condition` to DefaultExpectationConfigurationBuilder (#4246)
* [BUGFIX] Ensure that test result for `RegexPatternStringParameterBuilder` is deterministic (#4240)
* [BUGFIX] Remove duplicate RegexPatternStringParameterBuilder test (#4241)
* [BUGFIX] Improve pandas version checking in test_expectations[_cfe].py files (#4248)
* [BUGFIX] Ensure `test_script_runner.py` actually raises AssertionErrors correctly (#4239)
* [BUGFIX] Check for pandas>=024 not pandas>=24 (#4263)
* [BUGFIX] Add support for SqlAlchemyQueryStore connection_string credentials (#4224) (thanks @davidvanrooij)
* [BUGFIX] Remove assertion (#4271)
* [DOCS] Hackathon Contribution Docs (#3897)
* [MAINTENANCE] Rule-Based Profiler: Fix Circular Imports; Configuration Schema Fixes; Enhanced Unit Tests; Pre-Requisites/Refactoring for Self-Estimating Expectations (#4234)
* [MAINTENANCE] Reformat contrib expectation with black (#4244)
* [MAINTENANCE] Resolve cyclic import issue with usage stats (#4251)
* [MAINTENANCE] Additional refactor to clean up cyclic imports in usage stats (#4256)
* [MAINTENANCE] Rule-Based Profiler prerequisite: fix quantiles profiler configuration and add comments (#4255)
* [MAINTENANCE] Introspect Batch Request Dictionary for its kind and instantiate accordingly (#4259)
* [MAINTENANCE] Minor clean up in style of an RBP test fixture; making variables access more robust (#4261)
* [MAINTENANCE] define empty sqla_bigquery object (#4249)

0.14.7
-----------------
* [FEATURE] Support Multi-Dimensional Metric Computations Generically for Multi-Batch Parameter Builders (#4206)
* [FEATURE] Add support for sqlalchemy-bigquery while falling back on pybigquery (#4182)
* [BUGFIX] Update validate_configuration for core Expectations that don't return True (#4216)
* [DOCS] Fixes two references to the Getting Started tutorial (#4189)
* [DOCS] Deepnote Deployment Pattern Guide (#4169)
* [DOCS] Allow Data Docs to be rendered in night mode (#4130)
* [DOCS] Fix datepicker filter on data docs (#4217)
* [DOCS] Deepnote Deployment Pattern Image Fixes (#4229)
* [MAINTENANCE] Refactor RuleBasedProfiler toolkit pattern (#4191)
* [MAINTENANCE] Revert `dependency_graph` pipeline changes to ensure `usage_stats` runs in parallel (#4198)
* [MAINTENANCE] Refactor relative imports (#4195)
* [MAINTENANCE] Remove temp file that was accidently committed (#4201)
* [MAINTENANCE] Update default candidate strings SimpleDateFormatString parameter builder (#4193)
* [MAINTENANCE] minor type hints clean up (#4214)
* [MAINTENANCE] RBP testing framework changes (#4184)
* [MAINTENANCE] add conditional check for 'expect_column_values_to_be_in_type_list' (#4200)
* [MAINTENANCE] Allow users to pass in any set of polygon points in expectation for point to be within region (#2520) (thanks @ryanlindeborg)
* [MAINTENANCE] Better support Hive, better support BigQuery. (#2624) (thanks @jacobpgallagher)
* [MAINTENANCE] move process_evaluation_parameters into conditional (#4109)
* [MAINTENANCE] Type hint usage stats (#4226)

0.14.6
-----------------
* [FEATURE] Create profiler from DataContext (#4070)
* [FEATURE] Add read_sas function (#3972) (thanks @andyjessen)
* [FEATURE] Run profiler from DataContext (#4141)
* [FEATURE] Instantiate Rule-Based Profiler Using Typed Configuration Object (#4150)
* [FEATURE] Provide ability to instantiate Checkpoint using CheckpointConfig typed object (#4166)
* [FEATURE] Misc cleanup around CLI `suite` command and related utilities (#4158)
* [FEATURE] Add scheduled runs for primary Azure pipeline (#4117)
* [FEATURE] Promote dependency graph test strategy to production (#4124)
* [BUGFIX] minor updates to test definition json files (#4123)
* [BUGFIX] Fix typo for metric name in expect_column_values_to_be_edtf_parseable (#4140)
* [BUGFIX] Ensure that CheckpointResult object can be pickled (#4157)
* [BUGFIX] Custom notebook templates (#2619) (thanks @luke321321)
* [BUGFIX] Include public fields in property_names (#4159)
* [DOCS] Reenable docs-under-test for RuleBasedProfiler (#4149)
* [DOCS] Provided details for using GE_HOME in commandline. (#4164)
* [MAINTENANCE] Return Rule-Based Profiler base.py to its dedicated config subdirectory (#4125)
* [MAINTENANCE] enable filter properties dict to handle both inclusion and exclusion lists  (#4127)
* [MAINTENANCE] Remove unused Great Expectations imports (#4135)
* [MAINTENANCE] Update trigger for scheduled Azure runs (#4134)
* [MAINTENANCE] Maintenance/upgrade black (#4136)
* [MAINTENANCE] Alter `great_expectations` pipeline trigger to be more consistent (#4138)
* [MAINTENANCE] Remove remaining unused imports (#4137)
* [MAINTENANCE] Remove `class_name` as mandatory field from `RuleBasedProfiler` (#4139)
* [MAINTENANCE] Ensure `AWSAthena` does not create temporary table as part of processing Batch by default, which is currently not supported (#4103)
* [MAINTENANCE] Remove unused `Exception as e` instances (#4143)
* [MAINTENANCE] Standardize DictDot Method Behaviors Formally for Consistent Usage Patterns in Subclasses (#4131)
* [MAINTENANCE] Remove unused f-strings (#4142)
* [MAINTENANCE] Minor Validator code clean up -- for better code clarity (#4147)
* [MAINTENANCE] Refactoring of `test_script_runner.py`. Integration and Docs tests (#4145)
* [MAINTENANCE] Remove `compatability` stage from `dependency-graph` pipeline (#4161)
* [MAINTENANCE] CLOUD-618: GE Cloud "account" to "organization" rename (#4146)

0.14.5
-----------------
* [FEATURE] Delete profilers from DataContext (#4067)
* [FEATURE] [BUGFIX] Support nullable int column types (#4044) (thanks @scnerd)
* [FEATURE] Rule-Based Profiler Configuration and Runtime Arguments Reconciliation Logic (#4111)
* [BUGFIX] Add default BIGQUERY_TYPES (#4096)
* [BUGFIX] Pin `pip --upgrade` to a specific version for CI/CD pipeline (#4100)
* [BUGFIX] Use `pip==20.2.4` for usage statistics stage of CI/CD (#4102)
* [BUGFIX] Fix shared state issue in renderer test (#4000)
* [BUGFIX] Missing docstrings on validator expect_ methods (#4062) (#4081)
* [BUGFIX] Fix s3 path suffix bug on windows (#4042) (thanks @scnerd)
* [MAINTENANCE] fix typos in changelogs (#4093)
* [MAINTENANCE] Migration of GCP tests to new project (#4072)
* [MAINTENANCE] Refactor Validator methods (#4095)
* [MAINTENANCE] Fix Configuration Schema and Refactor Rule-Based Profiler; Initial Implementation of Reconciliation Logic Between Configuration and Runtime Arguments (#4088)
* [MAINTENANCE] Minor Cleanup -- remove unnecessary default arguments from dictionary cleaner (#4110)

0.14.4
-----------------
* [BUGFIX] Fix typing_extensions requirement to allow for proper build (#4083) (thanks @vojtakopal and @Godoy)
* [DOCS] data docs action rewrite (#4087)
* [DOCS] metric store how to rewrite (#4086)
* [MAINTENANCE] Change `logger.warn` to `logger.warning` to remove deprecation warnings (#4085)

0.14.3
-----------------
* [FEATURE] Profiler Store (#3990)
* [FEATURE] List profilers from DataContext (#4023)
* [FEATURE] add bigquery json credentials kwargs for sqlalchemy connect (#4039)
* [FEATURE] Get profilers from DataContext (#4033)
* [FEATURE] Add RuleBasedProfiler to `test_yaml_config` utility (#4038)
* [BUGFIX] Checkpoint Configurator fix to allow notebook logging suppression (#4057)
* [DOCS] Created a page containing our glossary of terms and definitions. (#4056)
* [DOCS] swap of old uri for new in data docs generated (#4013)
* [MAINTENANCE] Refactor `test_yaml_config` (#4029)
* [MAINTENANCE] Additional distinction made between V2 and V3 upgrade script (#4046)
* [MAINTENANCE] Correcting Checkpoint Configuration and Execution Implementation (#4015)
* [MAINTENANCE] Update minimum version for SQL Alchemy (#4055)
* [MAINTENANCE] Refactor RBP constructor to work with **kwargs instantiation pattern through config objects (#4043)
* [MAINTENANCE] Remove unnecessary metric dependency evaluations and add common table column types metric. (#4063)
* [MAINTENANCE] Clean up new RBP types, method signatures, and method names for the long term. (#4064)
* [MAINTENANCE] fixed broken function call in CLI (#4068)

0.14.2
-----------------
* [FEATURE] Marshmallow schema for Rule Based Profiler (#3982)
* [FEATURE] Enable Rule-Based Profile Parameter Access To Collection Typed Values (#3998)
* [BUGFIX] Docs integration pipeline bugfix  (#3997)
* [BUGFIX] Enables spark-native null filtering (#4004)
* [DOCS] Gtm/cta in docs (#3993)
* [DOCS] Fix incorrect variable name in how_to_configure_an_expectation_store_in_amazon_s3.md (#3971) (thanks @moritzkoerber)
* [DOCS] update custom docs css to add a subtle border around tabbed content (#4001)
* [DOCS] Migration Guide now includes example for Spark data (#3996)
* [DOCS] Revamp Airflow Deployment Pattern (#3963) (thanks @denimalpaca)
* [DOCS] updating redirects to reflect a moved file (#4007)
* [DOCS] typo in gcp + bigquery tutorial (#4018)
* [DOCS] Additional description of Kubernetes Operators in GCP Deployment Guide (#4019)
* [DOCS] Migration Guide now includes example for Databases (#4005)
* [DOCS] Update how to instantiate without a yml file (#3995)
* [MAINTENANCE] Refactor of `test_script_runner.py` to break-up test list (#3987)
* [MAINTENANCE] Small refactor for tests that allows DB setup to be done from all tests (#4012)

0.14.1
-----------------
* [FEATURE] Add pagination/search to CLI batch request listing (#3854)
* [BUGFIX] Safeguard against using V2 API with V3 Configuration (#3954)
* [BUGFIX] Bugfix and refactor for `cloud-db-integration` pipeline (#3977)
* [BUGFIX] Fixes breaking typo in expect_column_values_to_be_json_parseable (#3983)
* [BUGFIX] Fixes issue where nested columns could not be addressed properly in spark (#3986)
* [DOCS] How to connect to your data in `mssql` (#3950)
* [DOCS] MigrationGuide - Adding note on Migrating Expectation Suites (#3959)
* [DOCS] Incremental Update: The Universal Map's Getting Started Tutorial (#3881)
* [DOCS] Note about creating backup of Checkpoints (#3968)
* [DOCS] Connecting to BigQuery Doc line references fix (#3974)
* [DOCS] Remove RTD snippet about comments/suggestions from Docusaurus docs (#3980)
* [DOCS] Add howto for the OpenLineage validation operator (#3688) (thanks @rossturk)
* [DOCS] Updates to README.md (#3964)
* [DOCS] Update migration guide (#3967)
* [MAINTENANCE] Refactor docs dependency script (#3952)
* [MAINTENANCE] Use Effective SQLAlchemy for Reflection Fallback Logic and SQL Metrics (#3958)
* [MAINTENANCE] Remove outdated scripts (#3953)
* [MAINTENANCE] Add pytest opt to improve collection time (#3976)
* [MAINTENANCE] Refactor `render` method in PageRenderer (#3962)
* [MAINTENANCE] Standardize rule based profiler testing directories organization (#3984)
* [MAINTENANCE] Metrics Cleanup (#3989)
* [MAINTENANCE] Refactor `render` method of Content Block Renderer (#3960)

0.14.0
-----------------
* [BREAKING] Change Default CLI Flag To V3 (#3943)
* [FEATURE] Cloud-399/Cloud-519: Add Cloud Notification Action (#3891)
* [FEATURE] `great_expectations_contrib` CLI tool (#3909)
* [FEATURE] Update `dependency_graph` pipeline to use `dgtest` CLI (#3912)
* [FEATURE] Incorporate updated dgtest CLI tool in experimental pipeline (#3927)
* [FEATURE] Add YAML config option to disable progress bars (#3794)
* [BUGFIX] Fix internal links to docs that may be rendered incorrectly (#3915)
* [BUGFIX] Update SlackNotificationAction to send slack_token and slack_channel to send_slack_notification function (#3873) (thanks @Calvo94)
* [BUGFIX] `CheckDocsDependenciesChanges` to only handle `.py` files (#3936)
* [BUGFIX] Provide ability to capture schema_name for SQL-based datasources; fix method usage bugs. (#3938)
* [BUGFIX] Ensure that Jupyter Notebook cells convert JSON strings to Python-compliant syntax (#3939)
* [BUGFIX] Cloud-519/cloud notification action return type (#3942)
* [BUGFIX] Fix issue with regex groups in `check_docs_deps` (#3949)
* [DOCS] Created link checker, fixed broken links (#3930)
* [DOCS] adding the link checker to the build (#3933)
* [DOCS] Add name to link checker in build (#3935)
* [DOCS] GCP Deployment Pattern (#3926)
* [DOCS] remove v3api flag in documentation (#3944)
* [DOCS] Make corrections in HOWTO Guides for Getting Data from SQL Sources (#3945)
* [DOCS] Tiny doc fix (#3948)
* [MAINTENANCE] Fix breaking change caused by the new version of ruamel.yaml (#3908)
* [MAINTENANCE] Drop extraneous print statement in self_check/util.py. (#3905)
* [MAINTENANCE] Raise exceptions on init in cloud mode (#3913)
* [MAINTENANCE] removing commented requirement (#3920)
* [MAINTENANCE] Patch for atomic renderer snapshot tests (#3918)
* [MAINTENANCE] Remove types/expectations.py (#3928)
* [MAINTENANCE] Tests/test data class serializable dot dict (#3924)
* [MAINTENANCE] Ensure that concurrency is backwards compatible (#3872)
* [MAINTENANCE] Fix issue where meta was not recognized as a kwarg (#3852)

0.13.49
-----------------
* [FEATURE] PandasExecutionEngine is able to instantiate Google Storage client in Google Cloud Composer (#3896)
* [BUGFIX] Revert change to ExpectationSuite constructor (#3902)
* [MAINTENANCE] SQL statements that are of TextClause type expressed as subqueries (#3899)

0.13.48
-----------------
* [DOCS] Updates to configuring credentials (#3856)
* [DOCS] Add docs on creating suites with the UserConfigurableProfiler (#3877)
* [DOCS] Update how to configure an expectation store in GCS (#3874)
* [DOCS] Update how to configure a validation result store in GCS (#3887)
* [DOCS] Update how to host and share data docs on GCS (#3889)
* [DOCS] Organize metadata store sidebar category by type of store (#3890)
* [MAINTENANCE] `add_expectation()` in `ExpectationSuite` supports usage statistics for GE.  (#3824)
* [MAINTENANCE] Clean up Metrics type usage, SQLAlchemyExecutionEngine and SQLAlchemyBatchData implementation, and SQLAlchemy API usage (#3884)

0.13.47
-----------------
* [FEATURE] Add support for named groups in data asset regex (#3855)
* [BUGFIX] Fix issue where dependency graph tester picks up non *.py files and add test file (#3830)
* [BUGFIX] Ensure proper exit code for dependency graph script (#3839)
* [BUGFIX] Allows GE to work when installed in a zip file (PEP 273). Fixes issue #3772 (#3798) (thanks @joseignaciorc)
* [BUGFIX] Update conditional for TextClause isinstance check in SQLAlchemyExecutionEngine (#3844)
* [BUGFIX] Fix usage stats events (#3857)
* [BUGFIX] Make ExpectationContext optional and remove when null to ensure backwards compatability (#3859)
* [BUGFIX] Fix sqlalchemy expect_compound_columns_to_be_unique (#3827) (thanks @harperweaver-dox)
* [BUGFIX] Ensure proper serialization of SQLAlchemy Legacy Row (#3865)
* [DOCS] Update migration_guide.md (#3832)
* [MAINTENANCE] Remove the need for DataContext registry in the instrumentation of the Legacy Profiler profiling method. (#3836)
* [MAINTENANCE] Remove DataContext registry (#3838)
* [MAINTENANCE] Refactor cli suite conditionals (#3841)
* [MAINTENANCE] adding hints to stores in data context (#3849)
* [MAINTENANCE] Improve usage stats testing (#3858, #3861)
* [MAINTENANCE] Make checkpoint methods in DataContext pass-through (#3860)
* [MAINTENANCE] Datasource and ExecutionEngine Anonymizers handle missing module_name (#3867)
* [MAINTENANCE] Add logging around DatasourceInitializationError in DataContext (#3846)
* [MAINTENANCE] Use f-string to prevent string concat issue in Evaluation Parameters (#3864)
* [MAINTENANCE] Test for errors / invalid messages in logs & fix various existing issues (#3875)

0.13.46
-----------------
* [FEATURE] Instrument Runtime DataConnector for Usage Statistics: Add "checkpoint.run" Event Schema (#3797)
* [FEATURE] Add suite creation type field to CLI SUITE "new" and "edit" Usage Statistics events (#3810)
* [FEATURE] [EXPERIMENTAL] Dependency graph based testing strategy and related pipeline (#3738, #3815, #3818)
* [FEATURE] BaseDataContext registry (#3812, #3819)
* [FEATURE] Add usage statistics instrumentation to Legacy UserConfigurableProfiler execution (#3828)
* [BUGFIX] CheckpointConfig.__deepcopy__() must copy all fields, including the null-valued fields (#3793)
* [BUGFIX] Fix issue where configuration store didn't allow nesting (#3811)
* [BUGFIX] Fix Minor Bugs in and Clean Up UserConfigurableProfiler (#3822)
* [BUGFIX] Ensure proper replacement of nulls in Jupyter Notebooks (#3782)
* [BUGFIX] Fix issue where configuration store didn't allow nesting (#3811)
* [DOCS] Clean up TOC (#3783)
* [DOCS] Update Checkpoint and Actions Reference with testing (#3787)
* [DOCS] Update How to install Great Expectations locally (#3805)
* [DOCS] How to install Great Expectations in a hosted environment (#3808)
* [MAINTENANCE] Make BatchData Serialization More Robust (#3791)
* [MAINTENANCE] Refactor SiteIndexBuilder.build() (#3789)
* [MAINTENANCE] Update ref to ge-cla-bot in PR template (#3799)
* [MAINTENANCE] Anonymizer clean up and refactor (#3801)
* [MAINTENANCE] Certify the expectation "expect_table_row_count_to_equal_other_table" for V3 API (#3803)
* [MAINTENANCE] Refactor to enable broader use of event emitting method for usage statistics (#3825)
* [MAINTENANCE] Clean up temp file after CI/CD run (#3823)
* [MAINTENANCE] Raising exceptions for misconfigured datasources in cloud mode (#3866)

0.13.45
-----------------
* [FEATURE] Feature/render validation metadata (#3397) (thanks @vshind1)
* [FEATURE] Added expectation expect_column_values_to_not_contain_special_characters() (#2849, #3771) (thanks @jaibirsingh)
* [FEATURE] Like and regex-based expectations in Athena dialect (#3762) (thanks @josges)
* [FEATURE] Rename `deep_filter_properties_dict()` to `deep_filter_properties_iterable()`
* [FEATURE] Extract validation result failures (#3552) (thanks @BenGale93)
* [BUGFIX] Allow now() eval parameter to be used by itself (#3719)
* [BUGFIX] Fixing broken logo for legacy RTD docs (#3769)
* [BUGFIX] Adds version-handling to sqlalchemy make_url imports (#3768)
* [BUGFIX] Integration test to avoid regression of simple PandasExecutionEngine workflow (#3770)
* [BUGFIX] Fix copying of CheckpointConfig for substitution and printing purposes (#3759)
* [BUGFIX] Fix evaluation parameter usage with Query Store (#3763)
* [BUGFIX] Feature/fix row condition quotes (#3676) (thanks @benoitLebreton-perso)
* [BUGFIX] Fix incorrect filling out of anonymized event payload (#3780)
* [BUGFIX] Don't reset_index for conditional expectations (#3667) (thanks @abekfenn)
* [DOCS] Update expectations gallery link in V3 notebook documentation (#3747)
* [DOCS] Correct V3 documentation link in V2 notebooks to point to V2 documentation (#3750)
* [DOCS] How to pass an in-memory DataFrame to a Checkpoint (#3756)
* [MAINTENANCE] Fix typo in Getting Started Guide (#3749)
* [MAINTENANCE] Add proper docstring and type hints to Validator (#3767)
* [MAINTENANCE] Clean up duplicate logging statements about optional `black` dep (#3778)

0.13.44
-----------------
* [FEATURE] Add new result_format to include unexpected_row_list (#3346)
* [FEATURE] Implement "deep_filter_properties_dict()" method (#3703)
* [FEATURE] Create Constants for GETTING_STARTED Entities (e.g., datasource_name, expectation_suite_name, etc.) (#3712)
* [FEATURE] Add usage statistics event for DataContext.get_batch_list() method (#3708)
* [FEATURE] Add data_context.run_checkpoint event to usage statistics (#3721)
* [FEATURE] Add event_duration to usage statistics events (#3729)
* [FEATURE] InferredAssetSqlDataConnector's introspection can list external tables in Redshift Spectrum (#3646)
* [BUGFIX] Using a RuntimeBatchRequest in a Checkpoint with a top-level batch_request instead of validations (#3680)
* [BUGFIX] Using a RuntimeBatchRequest in a Checkpoint at runtime with Checkpoint.run() (#3713)
* [BUGFIX] Using a RuntimeBatchRequest in a Checkpoint at runtime with context.run_checkpoint() (#3718)
* [BUGFIX] Use SQLAlchemy make_url helper where applicable when parsing URLs (#3722)
* [BUGFIX] Adds check for quantile_ranges to be ordered or unbounded pairs (#3724)
* [BUGFIX] Updates MST renderer to return JSON-parseable boolean (#3728)
* [BUGFIX] Removes sqlite suppression for expect_column_quantile_values_to_be_between test definitions (#3735)
* [BUGFIX] Handle contradictory configurations in checkpoint.yml, checkpoint.run(), and context.run_checkpoint() (#3723)
* [BUGFIX] fixed a bug where expectation metadata doesn't appear in edit template for table-level expectations (#3129) (thanks @olechiw)
* [BUGFIX] Added temp_table creation for Teradata in SqlAlchemyBatchData (#3731) (thanks @imamolp)
* [DOCS] Add Databricks video walkthrough link (#3702, #3704)
* [DOCS] Update the link to configure a MetricStore (#3711, #3714) (thanks @txblackbird)
* [DOCS] Updated code example to remove deprecated "File" function (#3632) (thanks @daccorti)
* [DOCS] Delete how_to_add_a_validation_operator.md as OBE. (#3734)
* [DOCS] Update broken link in FOOTER.md to point to V3 documentation (#3745)
* [MAINTENANCE] Improve type hinting (using Optional type) (#3709)
* [MAINTENANCE] Standardize names for assets that are used in Getting Started Guide (#3706)
* [MAINTENANCE] Clean up remaining improper usage of Optional type annotation (#3710)
* [MAINTENANCE] Refinement of Getting Started Guide script (#3715)
* [MAINTENANCE] cloud-410 - Support for Column Descriptions (#3707)
* [MAINTENANCE] Types Clean Up in Checkpoint, Batch, and DataContext Classes (#3737)
* [MAINTENANCE] Remove DeprecationWarning for validator.remove_expectation (#3744)

0.13.43
-----------------
* [FEATURE] Enable support for Teradata SQLAlchemy dialect (#3496) (thanks @imamolp)
* [FEATURE] Dremio connector added (SQLalchemy) (#3624) (thanks @chufe-dremio)
* [FEATURE] Adds expect_column_values_to_be_string_integers_increasing (#3642)
* [FEATURE] Enable "column.quantile_values" and "expect_column_quantile_values_to_be_between" for SQLite; add/enable new tests (#3695)
* [BUGFIX] Allow glob_directive for DBFS Data Connectors (#3673)
* [BUGFIX] Update black version in pre-commit config (#3674)
* [BUGFIX] Make sure to add "mostly_pct" value if "mostly" kwarg present (#3661)
* [BUGFIX] Fix BatchRequest.to_json_dict() to not overwrite original fields; also type usage cleanup in CLI tests (#3683)
* [BUGFIX] Fix pyfakefs boto / GCS incompatibility (#3694)
* [BUGFIX] Update prefix attr assignment in cloud-based DataConnector constructors (#3668)
* [BUGFIX] Update 'list_keys' signature for all cloud-based tuple store child classes (#3669)
* [BUGFIX] evaluation parameters from different expectation suites dependencies (#3684) (thanks @OmriBromberg)
* [DOCS] Databricks deployment pattern documentation (#3682)
* [DOCS] Remove how_to_instantiate_a_data_context_on_databricks_spark_cluster (#3687)
* [DOCS] Updates to Databricks doc based on friction logging (#3696)
* [MAINTENANCE] Fix checkpoint anonymization and make BatchRequest.to_json_dict() more robust (#3675)
* [MAINTENANCE] Update kl_divergence domain_type (#3681)
* [MAINTENANCE] update filter_properties_dict to use set for inclusions and exclusions (instead of list) (#3698)
* [MAINTENANCE] Adds CITATION.cff (#3697)

0.13.42
-----------------
* [FEATURE] DBFS Data connectors (#3659)
* [BUGFIX] Fix "null" appearing in notebooks due to incorrect ExpectationConfigurationSchema serialization (#3638)
* [BUGFIX] Ensure that result_format from saved expectation suite json file takes effect (#3634)
* [BUGFIX] Allowing user specified run_id to appear in WarningAndFailureExpectationSuitesValidationOperator validation result (#3386) (thanks @wniroshan)
* [BUGFIX] Update black dependency to ensure passing Azure builds on Python 3.9 (#3664)
* [BUGFIX] fix Issue #3405 - gcs client init in pandas engine (#3408) (thanks @dz-1)
* [BUGFIX] Recursion error when passing RuntimeBatchRequest with query into Checkpoint using validations (#3654)
* [MAINTENANCE] Cloud 388/supported expectations query (#3635)
* [MAINTENANCE] Proper separation of concerns between specific File Path Data Connectors and corresponding ExecutionEngine objects (#3643)
* [MAINTENANCE] Enable Docusaurus tests for S3 (#3645)
* [MAINTENANCE] Formalize Exception Handling Between DataConnector and ExecutionEngine Implementations, and Update DataConnector Configuration Usage in Tests (#3644)
* [MAINTENANCE] Adds util for handling SADeprecation warning (#3651)

0.13.41
-----------------
* [FEATURE] Support median calculation in AWS Athena (#3596) (thanks @persiyanov)
* [BUGFIX] Be able to use spark execution engine with spark reuse flag (#3541) (thanks @fep2)
* [DOCS] punctuation how_to_contribute_a_new_expectation_to_great_expectations.md (#3484) (thanks @plain-jane-gray)
* [DOCS] Update next_steps.md (#3483) (thanks @plain-jane-gray)
* [DOCS] Update how_to_configure_a_validation_result_store_in_gcs.md (#3482) (thanks @plain-jane-gray)
* [DOCS] Choosing and configuring DataConnectors (#3533)
* [DOCS] Remove --no-spark flag from docs tests (#3625)
* [DOCS] DevRel - docs fixes (#3498)
* [DOCS] Adding a period (#3627) (thanks @plain-jane-gray)
* [DOCS] Remove comments that describe Snowflake parameters as optional (#3639)
* [MAINTENANCE] Update CODEOWNERS (#3604)
* [MAINTENANCE] Fix logo (#3598)
* [MAINTENANCE] Add Expectations to docs navbar (#3597)
* [MAINTENANCE] Remove unused fixtures (#3218)
* [MAINTENANCE] Remove unnecessary comment (#3608)
* [MAINTENANCE] Superconductive Warnings hackathon (#3612)
* [MAINTENANCE] Bring Core Skills Doc for Creating Batch Under Test (#3629)
* [MAINTENANCE] Refactor and Clean Up Expectations and Metrics Parts of the Codebase (better encapsulation, improved type hints) (#3633)


0.13.40
-----------------
* [FEATURE] Retrieve data context config through Cloud API endpoint #3586
* [FEATURE] Update Batch IDs to match name change in paths included in batch_request #3587
* [FEATURE] V2-to-V3 Upgrade/Migration #3592
* [FEATURE] table and graph atomic renderers #3595
* [FEATURE] V2-to-V3 Upgrade/Migration (Sidebar.js update) #3603
* [DOCS] Fixing broken links and linking to Expectation Gallery #3591
* [MAINTENANCE] Get TZLocal back to its original version control. #3585
* [MAINTENANCE] Add tests for datetime evaluation parameters #3601
* [MAINTENANCE] Removed warning for pandas option display.max_colwidth #3606

0.13.39
-----------------
* [FEATURE] Migration of Expectations to Atomic Prescriptive Renderers (#3530, #3537)
* [FEATURE] Cloud: Editing Expectation Suites programmatically (#3564)
* [BUGFIX] Fix deprecation warning for importing from collections (#3546) (thanks @shpolina)
* [BUGFIX] SQLAlchemy version 1.3.24 compatibility in map metric provider (#3507) (thanks @shpolina)
* [DOCS] Clarify how to configure optional Snowflake parameters in CLI datasource new notebook (#3543)
* [DOCS] Added breaks to code snippets, reordered guidance (#3514)
* [DOCS] typo in documentation (#3542) (thanks @DanielEdu)
* [DOCS] Update how_to_configure_a_new_data_context_with_the_cli.md (#3556) (thanks @plain-jane-gray)
* [DOCS] Improved installation instructions, included in-line installation instructions to getting started (#3509)
* [DOCS] Update contributing_style.md (#3521) (thanks @plain-jane-gray)
* [DOCS] Update contributing_test.md (#3519) (thanks @plain-jane-gray)
* [DOCS] Revamp style guides (#3554)
* [DOCS] Update contributing.md (#3523, #3524) (thanks @plain-jane-gray)
* [DOCS] Simplify getting started (#3555)
* [DOCS] How to introspect and partition an SQL database (#3465)
* [DOCS] Update contributing_checklist.md (#3518) (thanks @plain-jane-gray)
* [DOCS] Removed duplicate prereq, how_to_instantiate_a_data_context_without_a_yml_file.md (#3481) (thanks @plain-jane-gray)
* [DOCS] fix link to expectation glossary (#3558) (thanks @sephiartlist)
* [DOCS] Minor Friction (#3574)
* [MAINTENANCE] Make CLI Check-Config and CLI More Robust (#3562)
* [MAINTENANCE] tzlocal version fix (#3565)

0.13.38
-----------------
* [FEATURE] Atomic Renderer: Initial framework and Prescriptive renderers (#3529)
* [FEATURE] Atomic Renderer: Diagnostic renderers (#3534)
* [BUGFIX] runtime_parameters: {batch_data: <park DF} serialization (#3502)
* [BUGFIX] Custom query in RuntimeBatchRequest for expectations using table.row_count metric (#3508)
* [BUGFIX] Transpose \n and , in notebook (#3463) (thanks @mccalluc)
* [BUGFIX] Fix contributor link (#3462) (thanks @mccalluc)
* [DOCS] How to introspect and partition a files based data store (#3464)
* [DOCS] fixed duplication of text in code example (#3503)
* [DOCS] Make content better reflect the document organization. (#3510)
* [DOCS] Correcting typos and improving the language. (#3513)
* [DOCS] Better Sections Numbering in Documentation (#3515)
* [DOCS] Improved wording (#3516)
* [DOCS] Improved title wording for section heading (#3517)
* [DOCS] Improve Readability of Documentation Content (#3536)
* [MAINTENANCE] Content and test script update (#3532)
* [MAINTENANCE] Provide Deprecation Notice for the "parse_strings_as_datetimes" Expectation Parameter in V3 (#3539)

0.13.37
-----------------
* [FEATURE] Implement CompoundColumnsUnique metric for SqlAlchemyExecutionEngine (#3477)
* [FEATURE] add get_available_data_asset_names_and_types (#3476)
* [FEATURE] add s3_put_options to TupleS3StoreBackend (#3470) (Thanks @kj-9)
* [BUGFIX] Fix TupleS3StoreBackend remove_key bug (#3489)
* [DOCS] Adding Flyte Deployment pattern to docs (#3383)
* [DOCS] g_e docs branding updates (#3471)
* [MAINTENANCE] Add type-hints; add utility method for creating temporary DB tables; clean up imports; improve code readability; and add a directory to pre-commit (#3475)
* [MAINTENANCE] Clean up for a better code readability. (#3493)
* [MAINTENANCE] Enable SQL for the "expect_compound_columns_to_be_unique" expectation. (#3488)
* [MAINTENANCE] Fix some typos (#3474) (Thanks @mohamadmansourX)
* [MAINTENANCE] Support SQLAlchemy version 1.3.24 for compatibility with Airflow (Airflow does not currently support later versions of SQLAlchemy). (#3499)
* [MAINTENANCE] Update contributing_checklist.md (#3478) (Thanks @plain-jane-gray)
* [MAINTENANCE] Update how_to_configure_a_validation_result_store_in_gcs.md (#3480) (Thanks @plain-jane-gray)
* [MAINTENANCE] update implemented_expectations (#3492)

0.13.36
-----------------
* [FEATURE] GREAT-3439 extended SlackNotificationsAction for slack app tokens (#3440) (Thanks @psheets)
* [FEATURE] Implement Integration Test for "Simple SQL Datasource" with Partitioning, Splitting, and Sampling (#3454)
* [FEATURE] Implement Integration Test for File Path Data Connectors with Partitioning, Splitting, and Sampling (#3452)
* [BUGFIX] Fix Incorrect Implementation of the "_sample_using_random" Sampling Method in SQLAlchemyExecutionEngine (#3449)
* [BUGFIX] Handle RuntimeBatchRequest passed to Checkpoint programatically (without yml) (#3448)
* [DOCS] Fix typo in command to create new checkpoint (#3434) (Thanks @joeltone)
* [DOCS] How to validate data by running a Checkpoint (#3436)
* [ENHANCEMENT] cloud-199 - Update Expectation and ExpectationSuite classes for GE Cloud (#3453)
* [MAINTENANCE] Does not test numpy.float128 when it doesn't exist (#3460)
* [MAINTENANCE] Remove Unnecessary SQL OR Condition (#3469)
* [MAINTENANCE] Remove validation playground notebooks (#3467)
* [MAINTENANCE] clean up type hints, API usage, imports, and coding style (#3444)
* [MAINTENANCE] comments (#3457)

0.13.35
-----------------
* [FEATURE] Create ExpectationValidationGraph class to Maintain Relationship Between Expectation and Metrics and Use it to Associate Exceptions to Expectations (#3433)
* [BUGFIX] Addresses issue #2993 (#3054) by using configuration when it is available instead of discovering keys (listing keys) in existing sources. (#3377)
* [BUGFIX] Fix Data asset name rendering (#3431) (Thanks @shpolina)
* [DOCS] minor fix to syntax highlighting in how_to_contribute_a_new_expectation… (#3413) (Thanks @edjoesu)
* [DOCS] Fix broken links in how_to_create_a_new_expectation_suite_using_rule_based_profile… (#3410) (Thanks @edjoesu)
* [ENHANCEMENT] update list_expectation_suite_names and ExpectationSuiteValidationResult payload (#3419)
* [MAINTENANCE] Clean up Type Hints, JSON-Serialization, ID Generation and Logging in Objects in batch.py Module and its Usage (#3422)
* [MAINTENANCE] Fix Granularity of Exception Handling in ExecutionEngine.resolve_metrics() and Clean Up Type Hints (#3423)
* [MAINTENANCE] Fix broken links in how_to_create_a_new_expectation_suite_using_rule_based_profiler (#3441)
* [MAINTENANCE] Fix issue where BatchRequest object in configuration could cause Checkpoint to fail (#3438)
* [MAINTENANCE] Insure consistency between implementation of overriding Python __hash__() and internal ID property value (#3432)
* [MAINTENANCE] Performance improvement refactor for Spark unexpected values (#3368)
* [MAINTENANCE] Refactor MetricConfiguration out of validation_graph.py to Avoid Future Circular Dependencies in Python (#3425)
* [MAINTENANCE] Use ExceptionInfo to encapsulate common expectation validation result error information. (#3427)

0.13.34
-----------------
* [FEATURE] Configurable multi-threaded checkpoint speedup (#3362) (Thanks @jdimatteo)
* [BUGFIX] Insure that the "result_format" Expectation Argument is Processed Properly (#3364)
* [BUGFIX] fix error getting validation result from DataContext (#3359) (Thanks @zachzIAM)
* [BUGFIX] fixed typo and added CLA links (#3347)
* [DOCS] Azure Data Connector Documentation for Pandas and Spark. (#3378)
* [DOCS] Connecting to GCS using Spark (#3375)
* [DOCS] Docusaurus - Deploying Great Expectations in a hosted environment without file system or CLI (#3361)
* [DOCS] How to get a batch from configured datasource (#3382)
* [MAINTENANCE] Add Flyte to README (#3387) (Thanks @samhita-alla)
* [MAINTENANCE] Adds expect_table_columns_to_match_set (#3329) (Thanks @viniciusdsmello)
* [MAINTENANCE] Bugfix/skip substitute config variables in ge cloud mode (#3393)
* [MAINTENANCE] Clean Up ValidationGraph API Usage, Improve Exception Handling for Metrics, Clean Up Type Hints (#3399)
* [MAINTENANCE] Clean up ValidationGraph API and add Type Hints (#3392)
* [MAINTENANCE] Enhancement/update _set methods with kwargs (#3391) (Thanks @roblim)
* [MAINTENANCE] Fix incorrect ToC section name (#3395)
* [MAINTENANCE] Insure Correct Processing of the catch_exception Flag in Metrics Resolution (#3360)
* [MAINTENANCE] exempt batch_data from a deep_copy operation on RuntimeBatchRequest (#3388)
* [MAINTENANCE] [WIP] Enhancement/cloud 169/update checkpoint.run for ge cloud (#3381)

0.13.33
-----------------
* [FEATURE] Implement InferredAssetAzureDataConnector with Support for Pandas and Spark Execution Engines (#3372)
* [FEATURE] Spark connecting to Google Cloud Storage (#3365)
* [FEATURE] SparkDFExecutionEngine can load data accessed by ConfiguredAssetAzureDataConnector (integration tests are included). (#3345)
* [FEATURE] [MER-293] GE Cloud Mode for DataContext (#3262) (Thanks @roblim)
* [BUGFIX] Allow for RuntimeDataConnector to accept custom query while suppressing temp table creation (#3335) (Thanks @NathanFarmer)
* [BUGFIX] Fix issue where multiple validators reused the same execution engine, causing a conflict in active batch (GE-3168) (#3222) (Thanks @jcampbell)
* [BUGFIX] Run batch_request dictionary through util function convert_to_json_serializable (#3349) (Thanks @NathanFarmer)
* [BUGFIX] added casting of numeric value to fix redshift issue #3293 (#3338) (Thanks @sariabod)
* [DOCS] Docusaurus - How to connect to an MSSQL database (#3353) (Thanks @NathanFarmer)
* [DOCS] GREAT-195 Docs remove all stubs and links to them (#3363)
* [MAINTENANCE] Update azure-pipelines-docs-integration.yml for Azure Pipelines
* [MAINTENANCE] Update implemented_expectations.md (#3351) (Thanks @spencerhardwick)
* [MAINTENANCE] Updating to reflect current Expectation dev state (#3348) (Thanks @spencerhardwick)
* [MAINTENANCE] docs: Clean up Docusaurus refs (#3371)

0.13.32
-----------------
* [FEATURE] Add Performance Benchmarks Using BigQuery. (Thanks @jdimatteo)
* [WIP] [FEATURE] add backend args to run_diagnostics (#3257) (Thanks @edjoesu)
* [BUGFIX] Addresses Issue 2937. (#3236) (Thanks @BenGale93)
* [BUGFIX] SQL dialect doesn't register for BigQuery for V2 (#3324)
* [DOCS] "How to connect to data on GCS using Pandas" (#3311)
* [MAINTENANCE] Add CODEOWNERS with a single check for sidebars.js (#3332)
* [MAINTENANCE] Fix incorrect DataConnector usage of _get_full_file_path() API method. (#3336)
* [MAINTENANCE] Make Pandas against S3 and GCS integration tests more robust by asserting on number of batches returned and row counts (#3341)
* [MAINTENANCE] Make integration tests of Pandas against Azure more robust. (#3339)
* [MAINTENANCE] Prepare AzureUrl to handle WASBS format (for Spark) (#3340)
* [MAINTENANCE] Renaming default_batch_identifier in examples #3334
* [MAINTENANCE] Tests for RuntimeDataConnector at DataContext-level (#3304)
* [MAINTENANCE] Tests for RuntimeDataConnector at DataContext-level (Spark and Pandas) (#3325)
* [MAINTENANCE] Tests for RuntimeDataConnector at Datasource-level (Spark and Pandas) (#3318)
* [MAINTENANCE] Various doc patches (#3326)
* [MAINTENANCE] clean up imports and method signatures (#3337)

0.13.31
-----------------
* [FEATURE] Enable `GCS DataConnector` integration with `PandasExecutionEngine` (#3264)
* [FEATURE] Enable column_pair expectations and tests for Spark (#3294)
* [FEATURE] Implement `InferredAssetGCSDataConnector` (#3284)
* [FEATURE]/CHANGE run time format (#3272) (Thanks @serialbandicoot)
* [DOCS] Fix misc errors in "How to create renderers for Custom Expectations" (#3315)
* [DOCS] GDOC-217 remove stub links (#3314)
* [DOCS] Remove misc TODOs to tidy up docs (#3313)
* [DOCS] Standardize capitalization of various technologies in `docs` (#3312)
* [DOCS] Fix broken link to Contributor docs (#3295) (Thanks @discdiver)
* [MAINTENANCE] Additional tests for RuntimeDataConnector at Datasource-level (query) (#3288)
* [MAINTENANCE] Update GCSStoreBackend + tests (#2630) (Thanks @hmandsager)
* [MAINTENANCE] Write integration/E2E tests for `ConfiguredAssetAzureDataConnector` (#3204)
* [MAINTENANCE] Write integration/E2E tests for both `GCSDataConnectors` (#3301)

0.13.30
-----------------
* [FEATURE] Implement Spark Decorators and Helpers; Demonstrate on MulticolumnSumEqual Metric (#3289)
* [FEATURE] V3 implement expect_column_pair_values_to_be_in_set for SQL Alchemy execution engine (#3281)
* [FEATURE] Implement `ConfiguredAssetGCSDataConnector` (#3247)
* [BUGFIX] Fix import issues around cloud providers (GCS/Azure/S3) (#3292)
* [MAINTENANCE] Add force_reuse_spark_context to DatasourceConfigSchema (#3126) (thanks @gipaetusb and @mbakunze)

0.13.29
-----------------
* [FEATURE] Implementation of the Metric "select_column_values.unique.within_record" for SQLAlchemyExecutionEngine (#3279)
* [FEATURE] V3 implement ColumnPairValuesInSet for SQL Alchemy execution engine (#3278)
* [FEATURE] Edtf with support levels (#2594) (thanks @mielvds)
* [FEATURE] V3 implement expect_column_pair_values_to_be_equal for SqlAlchemyExecutionEngine (#3267)
* [FEATURE] add expectation for discrete column entropy  (#3049) (thanks @edjoesu)
* [FEATURE] Add SQLAlchemy Provider for the the column_pair_values.a_greater_than_b (#3268)
* [FEATURE] Expectations tests for BigQuery backend (#3219) (Thanks @jdimatteo)
* [FEATURE] Add schema validation for different GCS auth methods (#3258)
* [FEATURE] V3 - Implement column_pair helpers/providers for SqlAlchemyExecutionEngine (#3256)
* [FEATURE] V3 implement expect_column_pair_values_to_be_equal expectation for PandasExecutionEngine (#3252)
* [FEATURE] GCS DataConnector schema validation (#3253)
* [FEATURE] Implementation of the "expect_select_column_values_to_be_unique_within_record" Expectation (#3251)
* [FEATURE] Implement the SelectColumnValuesUniqueWithinRecord metric (for PandasExecutionEngine) (#3250)
* [FEATURE] V3 - Implement ColumnPairValuesEqual for PandasExecutionEngine (#3243)
* [FEATURE] Set foundation for GCS DataConnectors (#3220)
* [FEATURE] Implement "expect_column_pair_values_to_be_in_set" expectation (support for PandasExecutionEngine) (#3242)
* [BUGFIX] Fix deprecation warning for importing from collections (#3228) (thanks @ismaildawoodjee)
* [DOCS] Document BigQuery test dataset configuration (#3273) (Thanks @jdimatteo)
* [DOCS] Syntax and Link (#3266)
* [DOCS] API Links and Supporting Docs (#3265)
* [DOCS] redir and search (#3249)
* [MAINTENANCE] Update azure-pipelines-docs-integration.yml to include env vars for Azure docs integration tests
* [MAINTENANCE] Allow Wrong ignore_row_if Directive from V2 with Deprecation Warning (#3274)
* [MAINTENANCE] Refactor test structure for "Connecting to your data" cloud provider integration tests (#3277)
* [MAINTENANCE] Make test method names consistent for Metrics tests (#3254)
* [MAINTENANCE] Allow `PandasExecutionEngine` to accept `Azure DataConnectors` (#3214)
* [MAINTENANCE] Standardize Arguments to MetricConfiguration Constructor; Use {} instead of dict(). (#3246)

0.13.28
-----------------
* [FEATURE] Implement ColumnPairValuesInSet metric for PandasExecutionEngine
* [BUGFIX] Wrap optional azure imports in data_connector setup

0.13.27
-----------------
* [FEATURE] Accept row_condition (with condition_parser) and ignore_row_if parameters for expect_multicolumn_sum_to_equal (#3193)
* [FEATURE] ConfiguredAssetDataConnector for Azure Blob Storage (#3141)
* [FEATURE] Replace MetricFunctionTypes.IDENTITY domain type with convenience method get_domain_records() for SparkDFExecutionEngine (#3226)
* [FEATURE] Replace MetricFunctionTypes.IDENTITY domain type with convenience method get_domain_records() for SqlAlchemyExecutionEngine (#3215)
* [FEATURE] Replace MetricFunctionTypes.IDENTITY domain type with convenience method get_full_access_compute_domain() for PandasExecutionEngine (#3210)
* [FEATURE] Set foundation for Azure-related DataConnectors (#3188)
* [FEATURE] Update ExpectCompoundColumnsToBeUnique for V3 API (#3161)
* [BUGFIX] Fix incorrect schema validation for Azure data connectors (#3200)
* [BUGFIX] Fix incorrect usage of "all()" in the comparison of validation results when executing an Expectation (#3178)
* [BUGFIX] Fixes an error with expect_column_values_to_be_dateutil_parseable (#3190)
* [BUGFIX] Improve parsing of .ge_store_backend_id (#2952)
* [BUGFIX] Remove fixture parameterization for Cloud DBs (Snowflake and BigQuery) (#3182)
* [BUGFIX] Restore support for V2 API style custom expectation rendering (#3179) (Thanks @jdimatteo)
* [DOCS] Add `conda` as installation option in README (#3196) (Thanks @rpanai)
* [DOCS] Standardize capitalization of "Python" in "Connecting to your data" section of new docs (#3209)
* [DOCS] Standardize capitalization of Spark in docs (#3198)
* [DOCS] Update BigQuery docs to clarify the use of temp tables (#3184)
* [DOCS] Create _redirects (#3192)
* [ENHANCEMENT] RuntimeDataConnector messaging is made more clear for `test_yaml_config()` (#3206)
* [MAINTENANCE] Add `credentials` YAML key support for `DataConnectors` (#3173)
* [MAINTENANCE] Fix minor typo in S3 DataConnectors (#3194)
* [MAINTENANCE] Fix typos in argument names and types (#3207)
* [MAINTENANCE] Update changelog. (#3189)
* [MAINTENANCE] Update documentation. (#3203)
* [MAINTENANCE] Update validate_your_data.md (#3185)
* [MAINTENANCE] update tests across execution engines and clean up coding patterns (#3223)

0.13.26
-----------------
* [FEATURE] Enable BigQuery tests for Azure CI/CD (#3155)
* [FEATURE] Implement MulticolumnMapExpectation class (#3134)
* [FEATURE] Implement the MulticolumnSumEqual Metric for PandasExecutionEngine (#3130)
* [FEATURE] Support row_condition and ignore_row_if Directives Combined for PandasExecutionEngine (#3150)
* [FEATURE] Update ExpectMulticolumnSumToEqual for V3 API (#3136)
* [FEATURE] add python3.9 to python versions (#3143) (Thanks @dswalter)
* [FEATURE]/MER-16/MER-75/ADD_ROUTE_FOR_VALIDATION_RESULT (#3090) (Thanks @rreinoldsc)
* [BUGFIX] Enable `--v3-api suite edit` to proceed without selecting DataConnectors (#3165)
* [BUGFIX] Fix error when `RuntimeBatchRequest` is passed to `SimpleCheckpoint` with `RuntimeDataConnector` (#3152)
* [BUGFIX] allow reader_options in the CLI so can read `.csv.gz` files (#2695) (Thanks @luke321321)
* [DOCS] Apply Docusaurus tabs to relevant pages in new docs
* [DOCS] Capitalize python to Python in docs (#3176)
* [DOCS] Improve Core Concepts - Expectation Concepts (#2831)
* [MAINTENANCE] Error messages must be friendly. (#3171)
* [MAINTENANCE] Implement the "compound_columns_unique" metric for PandasExecutionEngine (with a unit test). (#3159)
* [MAINTENANCE] Improve Coding Practices in "great_expectations/expectations/expectation.py" (#3151)
* [MAINTENANCE] Update test_script_runner.py (#3177)

0.13.25
-----------------
* [FEATURE] Pass on meta-data from expectation json to validation result json (#2881) (Thanks @sushrut9898)
* [FEATURE] Add sqlalchemy engine support for `column.most_common_value` metric (#3020) (Thanks @shpolina)
* [BUGFIX] Added newline to CLI message for consistent formatting (#3127) (Thanks @ismaildawoodjee)
* [BUGFIX] fix pip install snowflake build error with python 3.9 (#3119) (Thanks @jdimatteo)
* [BUGFIX] Populate (data) asset name in data docs for RuntimeDataConnector (#3105) (Thanks @ceshine)
* [DOCS] Correct path to docs_rtd/changelog.rst (#3120) (Thanks @jdimatteo)
* [DOCS] Fix broken links in "How to write a 'How to Guide'" (#3112)
* [DOCS] Port over "How to add comments to Expectations and display them in DataDocs" from RTD to Docusaurus (#3078)
* [DOCS] Port over "How to create a Batch of data from an in memory Spark or Pandas DF" from RTD to Docusaurus (#3099)
* [DOCS] Update CLI codeblocks in create_your_first_expectations.md (#3106) (Thanks @ories)
* [MAINTENANCE] correct typo in docstring (#3117)
* [MAINTENANCE] DOCS/GDOC-130/Add Changelog (#3121)
* [MAINTENANCE] fix docstring for expectation "expect_multicolumn_sum_to_equal" (previous version was not precise) (#3110)
* [MAINTENANCE] Fix typos in docstrings in map_metric_provider partials (#3111)
* [MAINTENANCE] Make sure that all imports use column_aggregate_metric_provider (not column_aggregate_metric). (#3128)
* [MAINTENANCE] Rename column_aggregate_metric.py into column_aggregate_metric_provider.py for better code readability. (#3123)
* [MAINTENANCE] rename ColumnMetricProvider to ColumnAggregateMetricProvider (with DeprecationWarning) (#3100)
* [MAINTENANCE] rename map_metric.py to map_metric_provider.py (with DeprecationWarning) for a better code readability/interpretability (#3103)
* [MAINTENANCE] rename table_metric.py to table_metric_provider.py with a deprecation notice (#3118)
* [MAINTENANCE] Update CODE_OF_CONDUCT.md (#3066)
* [MAINTENANCE] Upgrade to modern Python syntax (#3068) (Thanks @cclauss)

0.13.24
-----------------
* [FEATURE] Script to automate proper triggering of Docs Azure pipeline (#3003)
* [BUGFIX] Fix an undefined name that could lead to a NameError (#3063) (Thanks @cclauss)
* [BUGFIX] fix incorrect pandas top rows usage (#3091)
* [BUGFIX] Fix parens in Expectation metric validation method that always returned True assertation (#3086) (Thanks @morland96)
* [BUGFIX] Fix run_diagnostics for contrib expectations (#3096)
* [BUGFIX] Fix typos discovered by codespell (#3064) (Thanks cclauss)
* [BUGFIX] Wrap get_view_names in try clause for passing the NotImplemented error (#2976) (Thanks @kj-9)
* [DOCS] Ensuring consistent style of directories, files, and related references in docs (#3053)
* [DOCS] Fix broken link to example DAG (#3061) (Thanks fritz-astronomer)
* [DOCS] GDOC-198 cleanup TOC (#3088)
* [DOCS] Migrating pages under guides/miscellaneous (#3094) (Thanks @spbail)
* [DOCS] Port over “How to configure a new Checkpoint using test_yaml_config” from RTD to Docusaurus
* [DOCS] Port over “How to configure an Expectation store in GCS” from RTD to Docusaurus (#3071)
* [DOCS] Port over “How to create renderers for custom Expectations” from RTD to Docusaurus
* [DOCS] Port over “How to run a Checkpoint in Airflow” from RTD to Docusaurus (#3074)
* [DOCS] Update how-to-create-and-edit-expectations-in-bulk.md (#3073)
* [MAINTENANCE] Adding a comment explaining the IDENTITY metric domain type. (#3057)
* [MAINTENANCE] Change domain key value from “column” to “column_list” in ExecutionEngine implementations (#3059)
* [MAINTENANCE] clean up metric errors (#3085)
* [MAINTENANCE] Correct the typo in the naming of the IDENTIFICATION semantic domain type name. (#3058)
* [MAINTENANCE] disable snowflake tests temporarily (#3093)
* [MAINTENANCE] [DOCS] Port over “How to host and share Data Docs on GCS” from RTD to Docusaurus (#3070)
* [MAINTENANCE] Enable repr for MetricConfiguration to assist with troubleshooting. (#3075)
* [MAINTENANCE] Expand test of a column map metric to underscore functionality. (#3072)
* [MAINTENANCE] Expectation anonymizer supports v3 expectation registry (#3092)
* [MAINTENANCE] Fix -- check for column key existence in accessor_domain_kwargsn for condition map partials. (#3082)
* [MAINTENANCE] Missing import of SparkDFExecutionEngine was added. (#3062)

0.13.23
-----------------
* [BUGFIX] added expectation_config to ExpectationValidationResult when exception is raised (#2659) (thanks @peterdhansen)
* [BUGFIX] fix update data docs as validation action (#3031)
* [DOCS] Port over "How to configure an Expectation Store in Azure" from RTD to Docusaurus
* [DOCS] Port over "How to host and share DataDocs on a filesystem" from RTD to Docusaurus (#3018)
* [DOCS] Port over "How to instantiate a Data Context w/o YML" from RTD to Docusaurus (#3011)
* [DOCS] Port "How to configure a Validation Result store on a filesystem" from RTD to Docusaurus (#3025)
* [DOCS] how to create multibatch expectations using evaluation parameters (#3039)
* [DOCS] Port "How to create and edit Expectations with a Profiler" from RTD to Docussaurus. (#3048)
* [DOCS] Port RTD adding validations data or suites to checkpoint (#3030)
* [DOCS] Porting "How to create and edit Expectations with instant feedback from a sample Batch of data" from RTD to Docusaurus. (#3046)
* [DOCS] GDOC-172/Add missing pages (#3007)
* [DOCS] Port over "How to configure DataContext components using test_yaml_config" from RTD to Docusaurus
* [DOCS] Port over "How to configure a Validation Result store to Postgres" from RTD to Docusaurus
* [DOCS] Port over "How to configure an Expectation Store in S3" from RTD to Docusaurus
* [DOCS] Port over "How to configure an Expectation Store on a filesystem" from RTD to Docusaurus
* [DOCS] Port over "How to configure credentials using YAML or env vars" from RTD to Docusaurus
* [DOCS] Port over "How to configure credentials using a secrets store" from RTD to Docusaurus
* [DOCS] Port over "How to configure validation result store in GCS" from RTD to Docusaurus (#3019)
* [DOCS] Port over "How to connect to an Athena DB" from RTD to Docusaurus
* [DOCS] Port over "How to create a new ExpectationSuite from jsonschema" from RTD to Docusaurus (#3017)
* [DOCS] Port over "How to deploy a scheduled checkpoint with cron" from RTD to Docusaurus
* [DOCS] Port over "How to dynamically load evaluation parameters from DB" from RTD to Docusaurus (#3052)
* [DOCS] Port over "How to host and share DataDocs on Amazon S3" from RTD to Docusaurus
* [DOCS] Port over "How to implement custom notifications" from RTD to Docusaurus  (#3050)
* [DOCS] Port over "How to instantiate a DataContext on Databricks Spark cluster" from RTD to Docusaurus
* [DOCS] Port over "How to instantiate a DataContext on an EMR Spark Cluster" from RTD to Docusaurus (#3024)
* [DOCS] Port over "How to trigger Opsgenie notifications as a validation action" from RTD to Docusaurus
* [DOCS] Update titles of metadata store docs (#3016)
* [DOCS] Port over "How to configure Expectation store to PostgreSQL" from RTD to Docusaurus (#3010)
* [DOCS] Port over "How to configure a MetricsStore" from RTD to Docusaurus (#3009)
* [DOCS] Port over "How to configure validation result store in Azure" from RTD to Docusaurus (#3014)
* [DOCS] Port over "How to host and share DataDocs on Azure" from RTD to Docusaurus  (#3012)
* [DOCS]Port "How to create and edit Expectations based on domain knowledge, without inspecting data directly" from RTD to Datasaurus. (#3047)
* [DOCS] Ported "How to configure a Validation Result store in Amazon S3" from RTD to Docusaurus. (#3026)
* [DOCS] how to validate without checkpoint (#3013)
* [DOCS] validation action data docs update (convert from RTD to DocuSaurus) (#3015)
* [DOCS] port of 'How to store Validation Results as a Validation Action' from RTD into Docusaurus. (#3023)
* [MAINTENANCE] Cleanup (#3038)
* [MAINTENANCE] Edits (Formatting) (#3022)


0.13.22
-----------------
* [FEATURE] Port over guide for Slack notifications for validation actions (#3005)
* [FEATURE] bootstrap estimator  for NumericMetricRangeMultiBatchParameterBuilder (#3001)
* [BUGFIX] Update naming of confidence_level in integration test fixture (#3002)
* [BUGFIX] [batch.py] fix check for null value (#2994) (thanks @Mohamed Abido)
* [BUGFIX] Fix issue where compression key was added to reader_method for read_parquet (#2506)
* [BUGFIX] Improve support for dates for expect_column_distinct_values_to_contain_set (#2997) (thanks @xaniasd)
* [BUGFIX] Fix bug in getting non-existent parameter (#2986)
* [BUGFIX] Modify read_excel() to handle new optional-dependency openpyxl for pandas >= 1.3.0 (#2989)
* [DOCS] Getting Started - Clean Up and Integration Tests (#2985)
* [DOCS] Adding in url links and style (#2999)
* [DOCS] Adding a missing import to a documentation page (#2983) (thanks @rishabh-bhargava)
* [DOCS]/GDOC-108/GDOC-143/Add in Contributing fields and updates (#2972)
* [DOCS] Update rule-based profiler docs (#2987)
* [DOCS] add image zoom plugin (#2979)
* [MAINTENANCE] fix lint issues for docusaurus (#3004)
* [Maintenance] update header to match GE.io (#2811)
* [MAINTENANCE] Instrument test_yaml_config() (#2981)
* [MAINTENANCE] Remove "mostly" from "bobster" test config (#2996)
* [MAINTENANCE] Update v-0.12 CLI test to reflect Pandas upgrade to version 1.3.0 (#2995)
* [MAINTENANCE] rephrase expectation suite meta profile comment (#2991)
* [MAINTENANCE] make citation cleaner in expectation suite (#2990)
* [MAINTENANCE] Attempt to fix Numpy and Scipy Version Requirements without additional requirements* files (#2982)

0.13.21
-----------------
* [DOCS] correct errors and reference complete example for custom expectations (thanks @jdimatteo)
* [DOCS] How to connect to : in-memory Pandas Dataframe
* [DOCS] How to connect to in memory dataframe with spark
* [DOCS] How to connect to : S3 data using Pandas
* [DOCS] How to connect to : Sqlite database
* [DOCS] no longer show util import to users
* [DOCS] How to connect to data on a filesystem using Spark guide
* [DOCS] GDOC-102/GDOC-127 Port in References and Tutorials
* [DOCS] How to connect to a MySQL database
* [DOCS] improved clarity in how to write guide templates and docs
* [DOCS] Add documentation for Rule Based Profilers
* [BUGFIX] Update mssql image version for Azure
* [MAINTENANCE] Update test-sqlalchemy-latest.yml
* [MAINTENANCE] Clean Up Design for Configuration and Flow of Rules, Domain Builders, and Parameter Builders
* [MAINTENANCE] Update Profiler docstring args
* [MAINTENANCE] Remove date format parameter builder
* [MAINTENANCE] Move metrics computations to top-level ParameterBuilder
* [MAINTENANCE] use tmp dot UUID for discardable expectation suite name
* [MAINTENANCE] Refactor ExpectationSuite to include profiler_config in citations
* [FEATURE] Add citations to Profiler.profile()
* [FEATURE] Bootstrapped Range Parameter Builder

0.13.20
-----------------
* [DOCS] Update pr template and remove enhancement feature type
* [DOCS] Remove broken links
* [DOCS] Fix typo in SlackNotificationAction docstring
* [BUGFIX] Update util.convert_to_json_serializable() to handle UUID type #2805 (thanks @YFGu0618)
* [BUGFIX] Allow decimals without leading zero in evaluation parameter URN
* [BUGFIX] Using cache in order not to fetch already known secrets #2882 (thanks @Cedric-Magnan)
* [BUGFIX] Fix creation of temp tables for unexpected condition
* [BUGFIX] Docs integration tests now only run when `--docs-tests` option is specified
* [BUGFIX] Fix instantiation of PandasExecutionEngine with custom parameters
* [BUGFIX] Fix rendering of observed value in datadocs when the value is 0 #2923 (thanks @shpolina)
* [BUGFIX] Fix serialization error in DataDocs rendering #2908 (thanks @shpolina)
* [ENHANCEMENT] Enable instantiation of a validator with a multiple batch BatchRequest
* [ENHANCEMENT] Adds a batch_request_list parameter to DataContext.get_validator to enable instantiation of a Validator with batches from multiple BatchRequests
* [ENHANCEMENT] Add a Validator.load_batch method to enable loading of additional Batches to an instantiated Validator
* [ENHANCEMENT] Experimental WIP Rule-Based Profiler for single batch workflows (#2788)
* [ENHANCEMENT] Datasources made via the CLI notebooks now include runtime and active data connector
* [ENHANCEMENT] InMemoryStoreBackendDefaults which is useful for testing
* [MAINTENANCE] Improve robustness of integration test_runner
* [MAINTENANCE] CLI tests now support click 8.0 and 7.x
* [MAINTENANCE] Soft launch of alpha docs site
* [MAINTENANCE] DOCS integration tests have moved to a new pipeline
* [MAINTENANCE] Pin json-schema version
* [MAINTENANCE] Allow tests to properly connect to local sqlite db on Windows (thanks @shpolina)
* [FEATURE] Add GeCloudStoreBackend with support for Checkpoints



0.13.19
-----------------
* [BUGFIX] Fix packaging error breaking V3 CLI suite commands (#2719)

0.13.18
-----------------
* [ENHANCEMENT] Improve support for quantiles calculation in Athena
* [ENHANCEMENT] V3 API CLI docs commands have better error messages and more consistent short flags
* [ENHANCEMENT] Update all Data Connectors to allow for `batch_spec_passthrough` in config
* [ENHANCEMENT] Update `DataConnector.build_batch_spec` to use `batch_spec_passthrough` in config
* [ENHANCEMENT] Update `ConfiguredAssetSqlDataConnector.build_batch_spec` and `ConfiguredAssetFilePathDataConnector.build_batch_spec` to properly process `Asset.batch_spec_passthrough`
* [ENHANCEMENT] Update `SqlAlchemyExecutionEngine.get_batch_data_and_markers` to handle `create_temp_table` in `RuntimeQueryBatchSpec`
* [ENHANCEMENT] Usage stats messages for the v3 API CLI are now sent before and after the command runs # 2661
* [ENHANCEMENT} Update the datasource new notebook for improved data asset inference
* [ENHANCEMENT] Update the `datasource new` notebook for improved data asset inference
* [ENHANCEMENT] Made stylistic improvements to the `checkpoint new` notebook
* [ENHANCEMENT] Add mode prompt to suite new and suite edit #2706
* [ENHANCEMENT] Update build_gallery.py script to better-handle user-submitted Expectations failing #2705
* [ENHANCEMENT] Docs + Tests for passing in reader_options to Spark #2670
* [ENHANCEMENT] Adding progressbar to validator loop #2620 (Thanks @peterdhansen!)
* [ENHANCEMENT] Great Expectations Compatibility with SqlAlchemy 1.4 #2641
* [ENHANCEMENT] Athena expect column quantile values to be between #2544 (Thanks @RicardoPedrotti!)
* [BUGFIX] Rename assets in SqlDataConnectors to be consistent with other DataConnectors #2665
* [BUGFIX] V3 API CLI docs build now opens all built sites rather than only the last one
* [BUGFIX] Handle limit for oracle with rownum #2691 (Thanks @NathanFarmer!)
* [BUGFIX] add create table logic for athena #2668 (Thanks @kj-9!)
* [BUGFIX] Add note for user-submitted Expectation that is not compatible with SqlAlchemy 1.4 (uszipcode) #2677
* [BUGFIX] Usage stats cli payload schema #2680
* [BUGFIX] Rename assets in SqlDataConnectors #2665
* [DOCS] Update how_to_create_a_new_checkpoint.rst with description of new CLI functionality
* [DOCS] Update Configuring Datasources documentation for V3 API CLI
* [DOCS] Update Configuring Data Docs documentation for V3 API CLI
* [DOCS] Update Configuring metadata stores documentation for V3 API CLI
* [DOCS] Update How to configure a Pandas/S3 Datasource for V3 API CLI
* [DOCS] Fix typos in "How to load a database table, view, or query result as a batch" guide and update with `create_temp_table` info
* [DOCS] Update "How to add a Validation Operator" guide to make it clear it is only for V2 API
* [DOCS] Update Version Migration Guide to recommend using V3 without caveats
* [DOCS] Formatting fixes for datasource docs #2686
* [DOCS] Add note about v3 API to How to use the Great Expectations command line interface (CLI) #2675
* [DOCS] CLI SUITE Documentation for V3 #2687
* [DOCS] how to share data docs on azure #2589 (Thanks @benoitLebreton-perso!)
* [DOCS] Fix typo in Core concepts/Key Ideas section #2660 (Thanks @svenhofstede!)
* [DOCS] typo in datasource documentation #2654 (Thanks @Gfeuillen!)
* [DOCS] fix grammar #2579 (Thanks @carlsonp!)
* [DOCS] Typo fix in Core Concepts/ Key Ideas section #2644 (Thanks @TremaMiguel!)
* [DOCS] Corrects wrong pypi package in Contrib Packages README #2653 (Thanks @mielvds!)
* [DOCS] Update dividing_data_assets_into_batches.rst #2651 (Thanks @lhayhurst!)
* [MAINTENANCE] Temporarily pin sqlalchemy (1.4.9) and add new CI stage #2708
* [MAINTENANCE] Run CLI tests as a separate stage in Azure pipelines #2672
* [MAINTENANCE] Updates to usage stats messages & tests for new CLI #2689
* [MAINTENANCE] Making user configurable profile test more robust; minor cleanup #2685
* [MAINTENANCE] remove cli.project.upgrade event #2682
* [MAINTENANCE] column reflection fallback should introspect one table (not all tables) #2657 (Thank you @peterdhansen!)
* [MAINTENANCE] Refactor Tests to Use Common Libraries #2663

0.13.17
-----------------
* [BREAKING-EXPERIMENTAL] The ``batch_data`` attribute of ``BatchRequest`` has been removed. To pass in in-memory dataframes at runtime, the new ``RuntimeDataConnector`` should be used
* [BREAKING-EXPERIMENTAL] ``RuntimeDataConnector`` must now be passed Batch Requests of type ``RuntimeBatchRequest``
* [BREAKING-EXPERIMENTAL] The ``PartitionDefinitionSubset`` class has been removed - the parent class ``IDDict`` is used in its place
* [BREAKING-EXPERIMENTAL] ``partition_request`` was renamed ``data_connector_query``. The related ``PartitionRequest`` class has been removed - the parent class ``IDDict`` is used in its place
* [BREAKING-EXPERIMENTAL] ``partition_definition`` was renamed ``batch_identifiers`. The related ``PartitionDefinition`` class has been removed - the parent class ``IDDict`` is used in its place
* [BREAKING-EXPERIMENTAL] The ``PartitionQuery`` class has been renamed to ``BatchFilter``
* [BREAKING-EXPERIMENTAL] The ``batch_identifiers`` key on ``DataConnectorQuery`` (formerly ``PartitionRequest``) has been changed to ``batch_filter_parameters``
* [ENHANCEMENT] Added a new ``RuntimeBatchRequest`` class, which can be used alongside ``RuntimeDataConnector`` to specify batches at runtime with either an in-memory dataframe, path (filesystem or s3), or sql query
* [ENHANCEMENT] Added a new ``RuntimeQueryBatchSpec`` class
* [ENHANCEMENT] CLI store list now lists active stores
* [BUGFIX] Fixed issue where Sorters were not being applied correctly when ``data_connector_query`` contained limit or index  #2617
* [DOCS] Updated docs to reflect above class name changes
* [DOCS] Added the following docs: "How to configure sorting in Data Connectors", "How to configure a Runtime Data Connector", "How to create a Batch Request using an Active Data Connector", "How to load a database table, view, or query result as a Batch"
* [DOCS] Updated the V3 API section of the following docs: "How to load a Pandas DataFrame as a Batch", "How to load a Spark DataFrame as a Batch",

0.13.16
-----------------
* [ENHANCEMENT] CLI `docs list` command implemented for v3 api #2612
* [MAINTENANCE] Add testing for overwrite_existing in sanitize_yaml_and_save_datasource #2613
* [ENHANCEMENT] CLI `docs build` command implemented for v3 api #2614
* [ENHANCEMENT] CLI `docs clean` command implemented for v3 api #2615
* [ENHANCEMENT] DataContext.clean_data_docs now raises helpful errors #2621
* [ENHANCEMENT] CLI `init` command implemented for v3 api #2626
* [ENHANCEMENT] CLI `store list` command implemented for v3 api #2627

0.13.15
-----------------
* [FEATURE] Added support for references to secrets stores for AWS Secrets Manager, GCP Secret Manager and Azure Key Vault in `great_expectations.yml` project config file (Thanks @Cedric-Magnan!)
* [ENHANCEMENT] Datasource CLI functionality for v3 api and global --assume-yes flag #2590
* [ENHANCEMENT] Update UserConfigurableProfiler to increase tolerance for mostly parameter of nullity expectations
* [ENHANCEMENT] Adding tqdm to Profiler (Thanks @peterdhansen). New library in requirements.txt
* [ENHANCEMENT][MAINTENANCE] Use Metrics to Protect Against Wrong Column Names
* [BUGFIX] Remove parentheses call at os.curdir in data_context.py #2566 (thanks @henriquejsfj)
* [BUGFIX] Sorter Configuration Added to DataConnectorConfig and DataConnectorConfigSchema #2572
* [BUGFIX] Remove autosave of Checkpoints in test_yaml_config and store SimpleCheckpoint as Checkpoint #2549
* [ENHANCE] Update UserConfigurableProfiler to increase tolerance for mostly parameter of nullity expectations
* [BUGFIX] Populate (data) asset name in data docs for SimpleSqlalchemy datasource (Thanks @xaniasd)
* [BUGFIX] pandas partial read_ functions not being unwrapped (Thanks @luke321321)
* [BUGFIX] Don't stop SparkContext when running in Databricks (#2587) (Thanks @jarandaf)
* [MAINTENANCE] Oracle listed twice in list of sqlalchemy dialects #2609
* [FEATURE] Oracle support added to sqlalchemy datasource and dataset #2609

0.13.14
-----------------
* [BUGFIX] Use temporary paths in tests #2545
* [FEATURE] Allow custom data_asset_name for in-memory dataframes #2494
* [ENHANCEMENT] Restore cli functionality for legacy checkpoints #2511
* [BUGFIX] Can not create Azure Backend with TupleAzureBlobStoreBackend #2513 (thanks @benoitLebreton-perso)
* [BUGFIX] force azure to set content_type='text/html' if the file is HTML #2539 (thanks @benoitLebreton-perso)
* [BUGFIX] Temporarily pin SqlAlchemy to < 1.4.0 in requirements-dev-sqlalchemy.txt #2547
* [DOCS] Fix documentation links generated within template #2542 (thanks @thejasraju)
* [MAINTENANCE] Remove deprecated automerge config #249

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
* added support for overriding the default jupyter command via a GE_JUPYTER_CMD environment variable (#1347) @nehiljain
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
* Fix an issue where expectation suite evaluation_parameters could be overridden by values during validate operation


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
