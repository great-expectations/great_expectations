Roadmap
=======
Updated March 2018

v0.5
---------------
* Support for meta variables
* Support for multi-column expectations

v0.6
---------------
* Datasources API Enhancements(Spark, Spark Streaming)
  - Support non-tabular datasources (e.g. JSON, XML, AVRO)
  - Real-time/streaming and adaption of distributional expectations
  - Database constraint/schema <-> expectation api


Changelog
=========

v.0.4.0
-----
* Initial implementation of data context API and SqlAlchemyDataSet including implementations of the following expectations:
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
* Major refactor of output_format to new result_obj_format parameter. See docs for full details.
  * exception_list and related uses of the term exception have been renamed to unexpected
  * the output formats are explicitly hierarchical now, with BOOLEAN_ONLY < BASIC < SUMMARY < COMPLETE. `column_aggregate_expectation`s now return element count and related information included at the BASIC level or higher.
* ge.from_pandas() utility (thanks @shrockn)
* Pandas operations on a PandasDataSet now return another PandasDataSet (thanks @dlwhite5)
* expect_column_to_exist now takes a column_index parameter to specify column order (thanks @louispotok)
* Top-level validate option (ge.validate())
* ge.read_json() helper (thanks @rjurney)
* Behind-the-scenes improvements to testing framework to ensure parity across data contexts.
* Documentation improvements, bug-fixes, and internal api improvements

v.0.3.2
-----
* Include requirements file in source dist to support conda

v.0.3.1
-----
* Fix infinite recursion error when building custom expectations
* Catch dateutil parsing overflow errors

0.2
---
* Distributional expectations and associated helpers are improved and renamed to be more clear regarding the tests they apply
* Expectation decorators have been refactored significantly to streamline implementing expectations and support custom expectations
* API and examples for custom expectations are available
* New output formats are available for all expectations
* Significant improvements to test suite and compatibility
