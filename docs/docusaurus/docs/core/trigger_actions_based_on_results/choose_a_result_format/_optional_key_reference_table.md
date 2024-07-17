The following table lists the valid keys for a Result Format dictionary and what their purpose is.  Not all keys are used by every verbosity level.

| Dictionary key | Purpose                                                                                                                                                                                                                                                                                                      |
| --- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`"result_format"` | Sets the fields to return in Validation Results.   Valid values are `"BASIC"`, `"BOOLEAN_ONLY"`, `"COMPLETE"`, and `"SUMMARY"`.  The default value is `"SUMMARY"`.                                                                                                                                           |
| `"unexpected_index_column_names"` | Defines the columns that can be used to identify unexpected results. For example, primary key (PK) column(s) or other columns with unique identifiers. Supports multiple column names as a list.                                                                                                             |
|`"return_unexpected_index_query"` | When running validations, a query (or a set of indices) is returned that allows you to retrieve the full set of unexpected results as well as the values of the identifying columns specified in `"unexpected_index_column_names"`.  Setting this value to `False` suppresses the output (default is `True`). |
| `"partial_unexpected_count"` | Sets the number of results to include in `"partial_unexpected_counts"`, `"partial_unexpected_list"`, and `"partial_unexpected_index_list"` if applicable. Set the value to zero to suppress the unexpected counts.                                                                                                                           |
| `"exclude_unexpected_values"` | When running validations, a set of unexpected results' indices and values is returned.  Setting this value to `True` suppresses values from the output to only have indices (default is `False`).                                                                                                            |
| `"include_unexpected_rows"` | When `True` this returns the entire row for each unexpected value in dictionary form. This setting only applies when `"result_format"` has been explicitly set to a value other than `"BOOLEAN_ONLY"`.                                                                                                       |

:::note
`include_unexpected_rows` returns EVERY row for each unexpected value. In large tables, this could result in an unmanageable amount of data.
:::