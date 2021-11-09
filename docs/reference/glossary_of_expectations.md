---
title: Glossary of Expectations
---

:::info WIP
While we work on autodoc API generation for our newer docs, the legacy docs are currently up to date.
:::

[API Reference Link](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/index.html#)

This is a list of all built-in Expectations. Expectations are extendable so you can create custom expectations for your data domain! To do so see this article: [How to create custom Expectations](/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_expectations).

:::tip

 Check out our new [Expectation Gallery](https://greatexpectations.io/expectations) for a more indepth view of each Expectation.

:::

## Dataset
Dataset objects model tabular data and include expectations with row and column semantics. Many Dataset expectations are implemented using column_map_expectation and column_aggregate_expectation decorators.

Not all expectations are currently implemented for each backend. Please see Table of Expectation Implementations By Backend.

### Table shape
```expect_column_to_exist```

```expect_table_columns_to_match_ordered_list```

```expect_table_columns_to_match_set```

```expect_table_row_count_to_be_between```

```expect_table_row_count_to_equal```

```expect_table_row_count_to_equal_other_table```

### Missing values, unique values, and types
```expect_column_values_to_be_unique```

```expect_column_values_to_not_be_null```

```expect_column_values_to_be_null```

```expect_column_values_to_be_of_type```

```expect_column_values_to_be_in_type_list```

### Sets and ranges
```expect_column_values_to_be_in_set```

```expect_column_values_to_not_be_in_set```

```expect_column_values_to_be_between```

```expect_column_values_to_be_increasing```

```expect_column_values_to_be_decreasing```

### String matching
```expect_column_value_lengths_to_be_between```

```expect_column_value_lengths_to_equal```

```expect_column_values_to_match_regex```

```expect_column_values_to_not_match_regex```

```expect_column_values_to_match_regex_list```

```expect_column_values_to_not_match_regex_list```

```expect_column_values_to_match_like_pattern```

```expect_column_values_to_not_match_like_pattern```

```expect_column_values_to_match_like_pattern_list```

```expect_column_values_to_not_match_like_pattern_list```

### Datetime and JSON parsing
```expect_column_values_to_match_strftime_format```

```expect_column_values_to_be_dateutil_parseable```

```expect_column_values_to_be_json_parseable```

```expect_column_values_to_match_json_schema```

### Aggregate functions
```expect_column_distinct_values_to_be_in_set```

```expect_column_distinct_values_to_contain_set```

```expect_column_distinct_values_to_equal_set```

```expect_column_mean_to_be_between```

```expect_column_median_to_be_between```

```expect_column_quantile_values_to_be_between```

```expect_column_stdev_to_be_between```

```expect_column_unique_value_count_to_be_between```

```expect_column_proportion_of_unique_values_to_be_between```

```expect_column_most_common_value_to_be_in_set```

```expect_column_max_to_be_between```

```expect_column_min_to_be_between```

```expect_column_sum_to_be_between```

### Multi-column
```expect_column_pair_values_A_to_be_greater_than_B```

```expect_column_pair_values_to_be_equal```

```expect_column_pair_values_to_be_in_set```

```expect_select_column_values_to_be_unique_within_record```

```expect_multicolumn_sum_to_equal```

```expect_column_pair_cramers_phi_value_to_be_less_than```

```expect_compound_columns_to_be_unique```

### Distributional functions
```expect_column_kl_divergence_to_be_less_than```

```expect_column_bootstrapped_ks_test_p_value_to_be_greater_than```

```expect_column_chisquare_test_p_value_to_be_greater_than```

```expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than```

### FileDataAsset
File data assets reason at the file level, and the line level (for text data).

```expect_file_line_regex_match_count_to_be_between```

```expect_file_line_regex_match_count_to_equal```

```expect_file_hash_to_equal```

```expect_file_size_to_be_between```

```expect_file_to_exist```

```expect_file_to_have_valid_table_header```

```expect_file_to_be_valid_json```


