---
title: Expectation implementations by backend
---


Because Great Expectations can run against different platforms, not all Expectations have been implemented
for all platforms. This table details which are implemented. Note we love pull-requests to help us fill
out the missing implementations!

|                                **Expectations**                                | **Pandas** | **SQL** | **Spark** |
---------------------------------------------------------------------------------|------------|---------|------------
|`expect_column_to_exist`                                                        | Y          | Y       | Y         |
|`expect_table_columns_to_match_ordered_list`                                    | Y          | Y       | Y         |
|`expect_table_columns_to_match_set`                                             | Y          | Y       | Y         |
|`expect_table_row_count_to_be_between`                                          | Y          | Y       | Y         |
|`expect_table_row_count_to_equal`                                               | Y          | Y       | Y         |
|`expect_table_row_count_to_equal_other_table`                                   | N          | Y       | N         |
|`expect_column_values_to_be_unique`                                             | Y          | Y       | Y         |
|`expect_column_values_to_not_be_null`                                           | Y          | Y       | Y         |
|`expect_column_values_to_be_null`                                               | Y          | Y       | Y         |
|`expect_column_values_to_be_of_type`                                            | Y          | Y       | Y         |
|`expect_column_values_to_be_in_type_list`                                       | Y          | Y       | Y         |
|`expect_column_values_to_be_in_set`                                             | Y          | Y       | Y         |
|`expect_column_values_to_not_be_in_set`                                         | Y          | Y       | Y         |
|`expect_column_values_to_be_between`                                            | Y          | Y       | Y         |
|`expect_column_values_to_be_increasing`                                         | Y          | N       | Y         |
|`expect_column_values_to_be_decreasing`                                         | Y          | N       | Y         |
|`expect_column_value_lengths_to_be_between`                                     | Y          | Y       | Y         |
|`expect_column_value_lengths_to_equal`                                          | Y          | Y       | Y         |
|`expect_column_values_to_match_regex`                                           | Y          | Y       | Y         |
|`expect_column_values_to_not_match_regex`                                       | Y          | Y       | Y         |
|`expect_column_values_to_match_regex_list`                                      | Y          | Y       | Y         |
|`expect_column_values_to_not_match_regex_list`                                  | Y          | Y       | Y         |
|`expect_column_values_to_match_like_pattern`                                    | N          | Y       | N         |
|`expect_column_values_to_not_match_like_pattern`                                | N          | Y       | N         |
|`expect_column_values_to_match_like_pattern_list`                               | N          | Y       | N         |
|`expect_column_values_to_not_match_like_pattern_list`                           | N          | Y       | N         |
|`expect_column_values_to_match_strftime_format`                                 | Y          | N       | Y         |
|`expect_column_values_to_be_dateutil_parseable`                                 | Y          | N       | N         |
|`expect_column_values_to_be_json_parseable`                                     | Y          | N       | Y         |
|`expect_column_values_to_match_json_schema`                                     | Y          | N       | Y         |
|`expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than` * | Y          | N       | N         |
|`expect_column_distinct_values_to_equal_set`                                    | Y          | Y       | Y         |
|`expect_column_distinct_values_to_contain_set`                                  | Y          | Y       | Y         |
|`expect_column_mean_to_be_between`                                              | Y          | Y       | Y         |
|`expect_column_median_to_be_between`                                            | Y          | Y       | Y         |
|`expect_column_stdev_to_be_between`                                             | Y          | N       | Y         |
|`expect_column_unique_value_count_to_be_between`                                | Y          | Y       | Y         |
|`expect_column_proportion_of_unique_values_to_be_between`                       | Y          | Y       | Y         |
|`expect_column_most_common_value_to_be_in_set`                                  | Y          | Y       | Y         |
|`expect_column_sum_to_be_between`                                               | Y          | Y       | Y         |
|`expect_column_min_to_be_between`                                               | Y          | Y       | Y         |
|`expect_column_max_to_be_between`                                               | Y          | Y       | Y         |
|`expect_column_chisquare_test_p_value_to_be_greater_than` *                     | Y          | Y       | Y         |
|`expect_column_bootstrapped_ks_test_p_value_to_be_greater_than` *               | Y          | N       | N         |
|`expect_column_kl_divergence_to_be_less_than`                                   | Y          | Y       | Y         |
|`expect_column_pair_values_to_be_equal`                                         | Y          | Y       | Y         |
|`expect_column_pair_values_A_to_be_greater_than_B`                              | Y          | Y       | Y         |
|`expect_column_pair_values_to_be_in_set`                                        | Y          | Y       | Y         |
|`expect_select_column_values_to_be_unique_within_record`                        | Y          | N       | Y         |
|`expect_compound_columns_to_be_unique`                                          | Y          | Y       | Y         |
|`expect_column_pair_cramers_phi_value_to_be_less_than` *                        | Y          | N       | N         |
|`expect_multicolumn_sum_to_equal`                                               | Y          | Y       | Y         |

`*` This Expectation has not yet been migrated to the v3 (Batch Request) API.
