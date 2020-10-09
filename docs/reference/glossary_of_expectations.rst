.. _expectation_glossary:

########################
Glossary of Expectations
########################

This is a list of all built-in Expectations. Expectations are extendable so you
can create custom expectations for your data domain! To do so see this article:
:ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations`.

*******
Dataset
*******

Dataset objects model tabular data and include expectations with row and column semantics. Many Dataset expectations
are implemented using column_map_expectation and column_aggregate_expectation decorators.

Not all expectations are currently implemented for each backend. Please see :ref:`implemented_expectations`.

Table shape
===========

* :func:`expect_column_to_exist <great_expectations.dataset.dataset.Dataset.expect_column_to_exist>`
* :func:`expect_table_columns_to_match_ordered_list <great_expectations.dataset.dataset.Dataset.expect_table_columns_to_match_ordered_list>`
* :func:`expect_table_columns_to_match_set <great_expectations.dataset.dataset.Dataset.expect_table_columns_to_match_set>`
* :func:`expect_table_row_count_to_be_between <great_expectations.dataset.dataset.Dataset.expect_table_row_count_to_be_between>`
* :func:`expect_table_row_count_to_equal <great_expectations.dataset.dataset.Dataset.expect_table_row_count_to_equal>`
* :func:`expect_table_row_count_to_equal_other_table <great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset.expect_table_row_count_to_equal_other_table>`

Missing values, unique values, and types
========================================

* :func:`expect_column_values_to_be_unique <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_unique>`
* :func:`expect_column_values_to_not_be_null <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_null>`
* :func:`expect_column_values_to_be_null <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_null>`
* :func:`expect_column_values_to_be_of_type <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_of_type>`
* :func:`expect_column_values_to_be_in_type_list <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_type_list>`

Sets and ranges
===============

* :func:`expect_column_values_to_be_in_set <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_set>`
* :func:`expect_column_values_to_not_be_in_set <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_in_set>`
* :func:`expect_column_values_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_between>`
* :func:`expect_column_values_to_be_increasing <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_increasing>`
* :func:`expect_column_values_to_be_decreasing <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_decreasing>`


String matching
===============

* :func:`expect_column_value_lengths_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_be_between>`
* :func:`expect_column_value_lengths_to_equal <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_equal>`
* :func:`expect_column_values_to_match_regex <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex>`
* :func:`expect_column_values_to_not_match_regex <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex>`
* :func:`expect_column_values_to_match_regex_list <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`
* :func:`expect_column_values_to_not_match_regex_list <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex_list>`
* :func:`expect_column_values_to_match_like_pattern <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_like_pattern>`
* :func:`expect_column_values_to_not_match_like_pattern <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_like_pattern>`
* :func:`expect_column_values_to_match_like_pattern_list <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_like_pattern_list>`
* :func:`expect_column_values_to_not_match_like_pattern_list <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_like_pattern_list>`

Datetime and JSON parsing
=========================

* :func:`expect_column_values_to_match_strftime_format <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_strftime_format>`
* :func:`expect_column_values_to_be_dateutil_parseable <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_dateutil_parseable>`
* :func:`expect_column_values_to_be_json_parseable <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_json_parseable>`
* :func:`expect_column_values_to_match_json_schema <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_json_schema>`

Aggregate functions
===================

* :func:`expect_column_distinct_values_to_be_in_set <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_be_in_set>`
* :func:`expect_column_distinct_values_to_contain_set <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_contain_set>`
* :func:`expect_column_distinct_values_to_equal_set <great_expectations.dataset.dataset.Dataset.expect_column_distinct_values_to_equal_set>`
* :func:`expect_column_mean_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_mean_to_be_between>`
* :func:`expect_column_median_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_median_to_be_between>`
* :func:`expect_column_quantile_values_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_quantile_values_to_be_between>`
* :func:`expect_column_stdev_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_stdev_to_be_between>`
* :func:`expect_column_unique_value_count_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_unique_value_count_to_be_between>`
* :func:`expect_column_proportion_of_unique_values_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_proportion_of_unique_values_to_be_between>`
* :func:`expect_column_most_common_value_to_be_in_set <great_expectations.dataset.dataset.Dataset.expect_column_most_common_value_to_be_in_set>`
* :func:`expect_column_max_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_max_to_be_between>`
* :func:`expect_column_min_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_min_to_be_between>`
* :func:`expect_column_sum_to_be_between <great_expectations.dataset.dataset.Dataset.expect_column_sum_to_be_between>`

Multi-column
============
* :func:`expect_column_pair_values_A_to_be_greater_than_B <great_expectations.dataset.dataset.Dataset.expect_column_pair_values_A_to_be_greater_than_B>`
* :func:`expect_column_pair_values_to_be_equal <great_expectations.dataset.dataset.Dataset.expect_column_pair_values_to_be_equal>`
* :func:`expect_column_pair_values_to_be_in_set <great_expectations.dataset.dataset.Dataset.expect_column_pair_values_to_be_in_set>`
* :func:`expect_select_column_values_to_be_unique_within_record <great_expectations.dataset.dataset.Dataset.expect_select_column_values_to_be_unique_within_record>`
* :func:`expect_multicolumn_sum_to_equal <great_expectations.dataset.dataset.Dataset.expect_multicolumn_sum_to_equal>`
* :func:`expect_column_pair_cramers_phi_value_to_be_less_than <great_expectations.dataset.dataset.Dataset.expect_column_pair_cramers_phi_value_to_be_less_than>`
* :func:`expect_compound_columns_to_be_unique <great_expectations.dataset.dataset.Dataset.expect_compound_columns_to_be_unique>`

Distributional functions
========================

* :func:`expect_column_kl_divergence_to_be_less_than <great_expectations.dataset.dataset.Dataset.expect_column_kl_divergence_to_be_less_than>`
* :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than <great_expectations.dataset.dataset.Dataset.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than>`
* :func:`expect_column_chisquare_test_p_value_to_be_greater_than <great_expectations.dataset.dataset.Dataset.expect_column_chisquare_test_p_value_to_be_greater_than>`
* :func:`expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than <great_expectations.dataset.dataset.Dataset.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than>`

*************
FileDataAsset
*************

File data assets reason at the file level, and the line level (for text data).

* :func:`expect_file_line_regex_match_count_to_be_between <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_line_regex_match_count_to_be_between>`
* :func:`expect_file_line_regex_match_count_to_equal <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_line_regex_match_count_to_equal>`
* :func:`expect_file_hash_to_equal <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_hash_to_equal>`
* :func:`expect_file_size_to_be_between <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_size_to_be_between>`
* :func:`expect_file_to_exist <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_to_exist>`
* :func:`expect_file_to_have_valid_table_header <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_to_have_valid_table_header>`
* :func:`expect_file_to_be_valid_json <great_expectations.data_asset.file_data_asset.FileDataAsset.expect_file_to_be_valid_json>`
