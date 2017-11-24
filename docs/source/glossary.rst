.. _glossary:

================================================================================
Glossary of Expectations
================================================================================

Table shape
--------------------------------------------------------------------------------

* :func:`expect_column_to_exist <great_expectations.dataset.base.DataSet.expect_column_to_exist>`
* :func:`expect_table_row_count_to_be_between <great_expectations.dataset.base.DataSet.expect_table_row_count_to_be_between>`
* :func:`expect_table_row_count_to_equal <great_expectations.dataset.base.DataSet.expect_table_row_count_to_equal>`

Missing values, unique values, and types
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_be_unique <great_expectations.dataset.base.DataSet.expect_column_values_to_be_unique>`
* :func:`expect_column_values_to_not_be_null <great_expectations.dataset.base.DataSet.expect_column_values_to_not_be_null>`
* :func:`expect_column_values_to_be_null <great_expectations.dataset.base.DataSet.expect_column_values_to_be_null>`
* :func:`expect_column_values_to_be_of_type <great_expectations.dataset.base.DataSet.expect_column_values_to_be_of_type>`

Sets and ranges
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_be_in_set <great_expectations.dataset.base.DataSet.expect_column_values_to_be_in_set>`
* :func:`expect_column_values_to_not_be_in_set <great_expectations.dataset.base.DataSet.expect_column_values_to_not_be_in_set>`
* :func:`expect_column_values_to_be_between <great_expectations.dataset.base.DataSet.expect_column_values_to_be_between>`

String matching
--------------------------------------------------------------------------------

* :func:`expect_column_value_lengths_to_be_between <great_expectations.dataset.base.DataSet.expect_column_value_lengths_to_be_between>`
* :func:`expect_column_values_to_match_regex <great_expectations.dataset.base.DataSet.expect_column_values_to_match_regex>`
* :func:`expect_column_values_to_not_match_regex <great_expectations.dataset.base.DataSet.expect_column_values_to_not_match_regex>`
* :func:`expect_column_values_to_match_regex_list <great_expectations.dataset.base.DataSet.expect_column_values_to_match_regex_list>`

*Named Regex Patterns*

.. code-block:: bash

	leading_whitespace :     ^[ \t\r\n]
	trailing_whitespace :    [ \t\r\n]$
	date :                   [1-2][0-9]{3}[-][0-1][0-9][-][0-3][0-9]
	phone_number :           [0-9]{10}
	state :                  [A-Z][A-Z]
	five_digit_zip_code :    [0-9]{5}
	nine_digit_zip_code :    [0-9]{9}
	name_suffix :            (JR|Jr|SR|Sr|II|III|IV)$
	name_like :              ^[A-Z][a-z]+$
	number_like :            ^\d+$
	email_like :             (^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)
	address_like :           \s*([0-9]*)\s((NW|SW|SE|NE|S|N|E|W))?(.*)((NW|SW|SE|NE|S|N|E|W))?((#|APT|BSMT|BLDG|DEPT|FL|FRNT|HNGR|KEY|LBBY|LOT|LOWR|OFC|PH|PIER|REAR|RM|SIDE|SLIP|SPC|STOP|STE|TRLR|UNIT|UPPR|\,)[^,]*)(\,)([\s\w]*)\n

Datetime and JSON parsing
--------------------------------------------------------------------------------

* :func:`expect_column_values_to_match_strftime_format <great_expectations.dataset.base.DataSet.expect_column_values_to_match_strftime_format>`
* :func:`expect_column_values_to_be_dateutil_parseable <great_expectations.dataset.base.DataSet.expect_column_values_to_be_dateutil_parseable>`
* :func:`expect_column_values_to_be_valid_json <great_expectations.dataset.base.DataSet.expect_column_values_to_be_valid_json>`
* :func:`expect_column_values_to_match_json_schema <great_expectations.dataset.base.DataSet.expect_column_values_to_match_json_schema>`

Aggregate functions
--------------------------------------------------------------------------------

* :func:`expect_column_mean_to_be_between <great_expectations.dataset.base.DataSet.expect_column_mean_to_be_between>`
* :func:`expect_column_median_to_be_between <great_expectations.dataset.base.DataSet.expect_column_median_to_be_between>`
* :func:`expect_column_stdev_to_be_between <great_expectations.dataset.base.DataSet.expect_column_stdev_to_be_between>`
* :func:`expect_column_unique_value_count_to_be_between <great_expectations.dataset.base.DataSet.expect_column_unique_value_count_to_be_between>`
* :func:`expect_column_proportion_of_unique_values_to_be_between <great_expectations.dataset.base.DataSet.expect_column_proportion_of_unique_values_to_be_between>`


Distributional functions
--------------------------------------------------------------------------------

* :func:`expect_column_kl_divergence_less_than <great_expectations.dataset.base.DataSet.expect_column_kl_divergence_less_than>`
* :func:`expect_column_bootstrapped_ks_test_p_value_greater_than <great_expectations.dataset.base.DataSet.expect_column_bootstrapped_ks_test_p_value_greater_than>`
* :func:`expect_column_chisquare_test_p_value_greater_than <great_expectations.dataset.base.DataSet.expect_column_chisquare_test_p_value_greater_than>`


Distributional function helpers
--------------------------------------------------------------------------------

* :func:`continuous_partition_data <great_expectations.dataset.util.partition_data>`
* :func:`categorical_partition_data <great_expectations.dataset.util.categorical_partition_data>`
* :func:`kde_partition_data <great_expectations.dataset.util.kde_smooth_data>`
* :func:`is_valid_partition_object <great_expectations.dataset.util.is_valid_partition_object>`
* :func:`is_valid_continuous_partition_object <great_expectations.dataset.util.is_valid_partition_object>`
* :func:`is_valid_categorical_partition_object <great_expectations.dataset.util.is_valid_partition_object>`
