---
title: Changelog
---

### Develop
* [BUGFIX] Fix deprecation warning for importing from collections (#3228)

### 0.13.28
* [FEATURE] Implement ColumnPairValuesInSet metric for PandasExecutionEngine
* [BUGFIX] Wrap optional azure imports in data_connector setup

### 0.13.27
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

### 0.13.26
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

### 0.13.25
* [FEATURE] Pass on meta-data from expectation json to validation result json (#2881) (Thanks @sushrut9898)
* [FEATURE] Add sqlalchemy engine support for `column.most_common_value` metric (#3020) (Thanks @shpolina)
* [BUGFIX] Added newline to CLI message for consistent formatting (#3127) (Thanks @ismaildawoodjee)
* [BUGFIX] fix pip install snowflake build error with Python 3.9 (#3119) (Thanks @jdimatteo)
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

### 0.13.24
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


### Older Changelist
Older changelist can be found at [https://github.com/great-expectations/great_expectations/blob/develop/docs_rtd/changelog.rst](https://github.com/great-expectations/great_expectations/blob/develop/docs_rtd/changelog.rst)
