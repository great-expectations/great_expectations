.. _glossary:

================================================================================
Glossary of Expectations
================================================================================

Table shape
--------------------------------------------------------------------------------

* :func:`expect_column_to_exist <great_expectations.dataset.base.Dataset.expect_column_to_exist>`
* :func:`expect_table_columns_to_match_ordered_list <great_expectations.dataset.base.Dataset.expect_table_columns_to_match_ordered_list>`
* :func:`expect_table_row_count_to_be_between <great_expectations.dataset.base.Dataset.expect_table_row_count_to_be_between>`
* :func:`expect_table_row_count_to_equal <great_expectations.dataset.base.Dataset.expect_table_row_count_to_equal>`

Missing values, unique values, and types
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_be_unique <great_expectations.dataset.base.Dataset.expect_column_values_to_be_unique>`
* :func:`expect_column_values_to_not_be_null <great_expectations.dataset.base.Dataset.expect_column_values_to_not_be_null>`
* :func:`expect_column_values_to_be_null <great_expectations.dataset.base.Dataset.expect_column_values_to_be_null>`
* :func:`expect_column_values_to_be_of_type <great_expectations.dataset.base.Dataset.expect_column_values_to_be_of_type>`
* :func:`expect_column_values_to_be_in_type_list <great_expectations.dataset.base.Dataset.expect_column_values_to_be_in_type_list>`

Sets and ranges
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_be_in_set <great_expectations.dataset.base.Dataset.expect_column_values_to_be_in_set>`
* :func:`expect_column_values_to_not_be_in_set <great_expectations.dataset.base.Dataset.expect_column_values_to_not_be_in_set>`
* :func:`expect_column_values_to_be_between <great_expectations.dataset.base.Dataset.expect_column_values_to_be_between>`
* :func:`expect_column_values_to_be_increasing <great_expectations.dataset.base.Dataset.expect_column_values_to_be_increasing>`
* :func:`expect_column_values_to_be_decreasing <great_expectations.dataset.base.Dataset.expect_column_values_to_be_decreasing>`


String matching
--------------------------------------------------------------------------------

* :func:`expect_column_value_lengths_to_be_between <great_expectations.dataset.base.Dataset.expect_column_value_lengths_to_be_between>`
* :func:`expect_column_values_to_match_regex <great_expectations.dataset.base.Dataset.expect_column_values_to_match_regex>`
* :func:`expect_column_values_to_not_match_regex <great_expectations.dataset.base.Dataset.expect_column_values_to_not_match_regex>`
* :func:`expect_column_values_to_match_regex_list <great_expectations.dataset.base.Dataset.expect_column_values_to_match_regex_list>`
* :func:`expect_column_values_to_not_match_regex_list <great_expectations.dataset.base.Dataset.expect_column_values_to_not_match_regex_list>`

Datetime and JSON parsing
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_match_strftime_format <great_expectations.dataset.base.Dataset.expect_column_values_to_match_strftime_format>`
* :func:`expect_column_values_to_be_dateutil_parseable <great_expectations.dataset.base.Dataset.expect_column_values_to_be_dateutil_parseable>`
* :func:`expect_column_values_to_be_json_parseable <great_expectations.dataset.base.Dataset.expect_column_values_to_be_json_parseable>`
* :func:`expect_column_values_to_match_json_schema <great_expectations.dataset.base.Dataset.expect_column_values_to_match_json_schema>`

Aggregate functions
--------------------------------------------------------------------------------

* :func:`expect_column_mean_to_be_between <great_expectations.dataset.base.Dataset.expect_column_mean_to_be_between>`
* :func:`expect_column_median_to_be_between <great_expectations.dataset.base.Dataset.expect_column_median_to_be_between>`
* :func:`expect_column_stdev_to_be_between <great_expectations.dataset.base.Dataset.expect_column_stdev_to_be_between>`
* :func:`expect_column_unique_value_count_to_be_between <great_expectations.dataset.base.Dataset.expect_column_unique_value_count_to_be_between>`
* :func:`expect_column_proportion_of_unique_values_to_be_between <great_expectations.dataset.base.Dataset.expect_column_proportion_of_unique_values_to_be_between>`
* :func:`expect_column_most_common_value_to_be <great_expectations.dataset.base.Dataset.expect_column_most_common_value_to_be>`
* :func:`expect_column_most_common_value_to_be_in_set <great_expectations.dataset.base.Dataset.expect_column_most_common_value_to_be_in_set>`


Distributional functions
--------------------------------------------------------------------------------

* :func:`expect_column_kl_divergence_to_be_less_than <great_expectations.dataset.base.Dataset.expect_column_kl_divergence_to_be_less_than>`
* :func:`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than <great_expectations.dataset.base.Dataset.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than>`
* :func:`expect_column_chisquare_test_p_value_to_be_greater_than <great_expectations.dataset.base.Dataset.expect_column_chisquare_test_p_value_to_be_greater_than>`


Distributional function helpers
--------------------------------------------------------------------------------

* :func:`continuous_partition_data <great_expectations.Dataset.util.partition_data>`
* :func:`categorical_partition_data <great_expectations.Dataset.util.categorical_partition_data>`
* :func:`kde_partition_data <great_expectations.Dataset.util.kde_smooth_data>`
* :func:`is_valid_partition_object <great_expectations.Dataset.util.is_valid_partition_object>`
* :func:`is_valid_continuous_partition_object <great_expectations.Dataset.util.is_valid_partition_object>`
* :func:`is_valid_categorical_partition_object <great_expectations.Dataset.util.is_valid_partition_object>`
