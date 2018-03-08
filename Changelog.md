Great Expectations Changelog

* Major refactor of output_format to new result_obj_format parameter. See docs for full details.
  * exception_list and related uses of the term exception have been renamed to unexpected
  * the output formats are explicitly hierarchical now, with BOOLEAN_ONLY < BASIC < SUMMARY < COMPLETE. `column_aggregate_expectation`s now return element count and related information included at the BASIC level or higher.

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
