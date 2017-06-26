.. _glossary:

================================================================================
Glossary of Expectations
================================================================================

Table shape
--------------------------------------------------------------------------------
* expect_table_to_have_column
* expect_table_row_count_to_be_between
* expect_two_table_row_counts_to_be_equal

Missing values, unique values, and types
--------------------------------------------------------------------------------

* expect_column_values_to_be_unique
* expect_column_values_to_not_be_null
* expect_column_values_to_be_null
* expect_column_values_to_be_of_type

Sets and ranges
--------------------------------------------------------------------------------

* expect_column_values_to_be_in_set
* expect_column_values_to_not_be_in_set
* expect_column_values_to_be_between

String matching
--------------------------------------------------------------------------------

* expect_column_value_lengths_to_be_between
* expect_column_values_to_match_regex
* expect_column_values_to_not_match_regex
* expect_column_values_to_match_regex_list


*Named Regex Patterns*

.. code-block:: bash

	leading_whitespace :     `^[ \t\r\n]`
	trailing_whitespace :    `[ \t\r\n]$`
	date :                   `[1-2][0-9]{3}[-][0-1][0-9][-][0-3][0-9]`
	phone_number :           `[0-9]{9}`
	state :                  `[A-Z][A-Z]`
	five_digit_zip_code :    `[0-9]{5}`
	nine_digit_zip_code :    `[0-9]{9}`
	name_suffix :            `(JR|III|II|SR|Jr|Sr)$`
	name_like :              `^[A-Z][a-z]+$`
	number_like :            `^\d+$`
	address_like :           `^(P ?O Box)|(\d+[ABCDE]? (S|W|N|E|NW|SW|NE|SE)? ?([A-Z][a-z]+|\d+rd|\d+th))`
	email :                  `^[A-Za-z0-9\\._\\-]*@[A-Za-z0-9\\-]*\\.(com|COM|NET|net|ORG|org|EDU|edu|GOV|gov)$`


Datetime and JSON parsing
--------------------------------------------------------------------------------
* expect_column_values_to_match_strftime_format
* expect_column_values_to_be_dateutil_parseable
* expect_column_values_to_be_valid_json
* expect_column_values_to_match_json_schema


Aggregate functions
--------------------------------------------------------------------------------
* expect_column_mean_to_be_between
* expect_column_median_to_be_between
* expect_column_stdev_to_be_between
* expect_column_numerical_distribution_to_be
* expect_column_frequency_distribution_to_be


Column pairs
--------------------------------------------------------------------------------
* expect_two_column_values_to_be_equal
* expect_two_column_values_to_be_subsets
* expect_two_column_values_to_be_many_to_one
* expect_two_column_crosstabs_to_be


Multicolumns relations
--------------------------------------------------------------------------------
* expect_multicolumn_values_to_be_unique

