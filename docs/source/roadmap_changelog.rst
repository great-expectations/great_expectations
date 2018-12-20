.. _roadmap_changelog:

Changelog and Roadmap
=====================

Planned Features
----------------
* More expectation coverage in SqlAlchemyDataset
* Improved variable typing
* New Datasets (e.g. Spark)
* Support for non-tabular datasources (e.g. JSON, XML, AVRO)
* Real-time/streaming and adaption of distributional expectations


v.0.4.5__develop
----------------

v.0.4.5
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
* Fix map expectations for categorical columns (thanks @eugmandel)
* Improve internal testing suite (thanks @anhollis and @ccnobbli)
* Consistently use value_set instead of mixing value_set and values_set (thanks @njsmith8)

v.0.4.4
----------------
* Improve CLI help and set CLI return value to the number of unmet expectations
* Add error handling for empty columns to SqlAlchemyDataset, and associated tests
* Fix broken support for older pandas versions (#346)
* Fix pandas deepcopy issue (#342)

v.0.4.3
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
* Fix PandasDataset subclasses to inherit child class

v.0.4.2
-------
* Fix bugs in expect_column_values_to_[not]_be_null: computing unexpected value percentages and handling all-null (thanks @ccnobbli)
* Support mysql use of Decimal type (thanks @bouke-nederstigt)
* Add new expectation expect_column_values_to_not_match_regex_list.
  * Change behavior of expect_column_values_to_match_regex_list to use python re.findall in PandasDataset, relaxing \
  matching of individuals expressions to allow matches anywhere in the string.
* Fix documentation errors and other small errors (thanks @roblim, @ccnobbli)

v.0.4.1
-------
* Correct inclusion of new data_context module in source distribution

v.0.4.0
-------
* Initial implementation of data context API and SqlAlchemyDataset including implementations of the following expectations:
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
* Major refactor of output_format to new result_format parameter. See docs for full details.
  * exception_list and related uses of the term exception have been renamed to unexpected
  * the output formats are explicitly hierarchical now, with BOOLEAN_ONLY < BASIC < SUMMARY < COMPLETE. `column_aggregate_expectation`s now return element count and related information included at the BASIC level or higher.
* New expectation available for parameterized distributions--expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than (what a name! :) -- (thanks @ccnobbli)
* ge.from_pandas() utility (thanks @schrockn)
* Pandas operations on a PandasDataset now return another PandasDataset (thanks @dlwhite5)
* expect_column_to_exist now takes a column_index parameter to specify column order (thanks @louispotok)
* Top-level validate option (ge.validate())
* ge.read_json() helper (thanks @rjurney)
* Behind-the-scenes improvements to testing framework to ensure parity across data contexts.
* Documentation improvements, bug-fixes, and internal api improvements

v.0.3.2
-------
* Include requirements file in source dist to support conda

v.0.3.1
--------
* Fix infinite recursion error when building custom expectations
* Catch dateutil parsing overflow errors

v.0.2
-----
* Distributional expectations and associated helpers are improved and renamed to be more clear regarding the tests they apply
* Expectation decorators have been refactored significantly to streamline implementing expectations and support custom expectations
* API and examples for custom expectations are available
* New output formats are available for all expectations
* Significant improvements to test suite and compatibility
