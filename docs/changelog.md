---
title: Changelog
---

### Develop
* [MAINTENANCE] Expectation anonymizer supports v3 expectation registry (#3092)
* [FEATURE] Add sqlalchemy engine support for `column.most_common_value` metric
* [BUGFIX] Populate (data) asset name in data docs for RuntimeDataConnector (#3105)

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
