---
title: Result format
---


The `result_format` parameter may be either a string or a dictionary which specifies the fields to return in `result`.
  * For string usage, see `result_format` values.
  * For dictionary usage, `result_format` which may include the following keys:
    * `result_format`: Sets the fields to return in result.
    * `partial_unexpected_count`: Sets the number of results to include in partial_unexpected_count, if applicable. If 
      set to 0, this will suppress the unexpected counts.
    * `include_unexpected_rows`: When running validations, this will return the entire row for each unexpected value in
      dictionary form. When using `include_unexpected_rows`, you must explicitly specify `result_format` as well, and
      `result_format` must be more verbose than `BOOLEAN_ONLY`. *WARNING: *

  :::warning
  `include_unexpected_rows` returns EVERY row for each unexpected value; for large tables, this could return an 
  unwieldy amount of data.
  :::


## result_format values

Great Expectations supports four values for `result_format`: `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, and `COMPLETE`. The 
out-of-the-box default is `BASIC`. Each successive value includes more detail and so can support different use 
cases for working with Great Expectations, including interactive exploratory work and automatic validation.


| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    element_count                      |no              |yes             |yes             |yes             |
|    missing_count                      |no              |yes             |yes             |yes             |
|    missing_percent                    |no              |yes             |yes             |yes             |
|    details (dictionary)               |Defined on a per-expectation basis                                 |
| Fields defined for `column_map_expectation` type expectations:                                            |
|    unexpected_count                   |no              |yes             |yes             |yes             |
|    unexpected_percent                 |no              |yes             |yes             |yes             |
|    unexpected_percent_nonmissing      |no              |yes             |yes             |yes             |
|    partial_unexpected_list            |no              |yes             |yes             |yes             |
|    partial_unexpected_index_list      |no              |no              |yes             |yes             |
|    partial_unexpected_counts          |no              |no              |yes             |yes             |
|    unexpected_index_list              |no              |no              |no              |yes             |
|    unexpected_list                    |no              |no              |no              |yes             |
| Fields defined for `column_aggregate_expectation` type expectations:                                      |
|    observed_value                     |no              |yes             |yes             |yes             |
|    details (e.g. statistical details) |no              |no              |yes             |yes             |

### Example use cases for different result_format values

| `result_format` Setting               | Example use case                                             |
----------------------------------------|---------------------------------------------------------------
|    BOOLEAN_ONLY                       | Automatic validation. No result is returned.                 |
|    BASIC                              | Exploratory analysis in a notebook.                          |
|    SUMMARY                            | Detailed exploratory work with follow-on investigation.      |
|    COMPLETE                           | Debugging pipelines or developing detailed regression tests. |

## result_format examples

Example input:
```python
print(list(my_df.my_var))
['A', 'B', 'B', 'C', 'C', 'C', 'D', 'D', 'D', 'D', 'E', 'E', 'E', 'E', 'E', 'F', 'F', 'F', 'F', 'F', 'F', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'H', 'H', 'H', 'H', 'H', 'H', 'H', 'H']
```

Example outputs for different values of `result_format`:


```python
my_df.expect_column_values_to_be_in_set(
    "my_var",
    ["B", "C", "D", "F", "G", "H"],
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
{
    'success': False
}
```

```python
my_df.expect_column_values_to_be_in_set(
    "my_var",
    ["B", "C", "D", "F", "G", "H"],
    result_format={'result_format': 'BASIC'}
)
{
    'success': False,
    'result': {
        'unexpected_count': 6,
        'unexpected_percent': 0.16666666666666666,
        'unexpected_percent_nonmissing': 0.16666666666666666,
        'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
    }
}
```

```python
expect_column_values_to_match_regex(
    "my_column",
    "[A-Z][a-z]+",
    result_format={'result_format': 'SUMMARY'}
)
{
    'success': False,
    'result': {
        'element_count': 36,
        'unexpected_count': 6,
        'unexpected_percent': 0.16666666666666666,
        'unexpected_percent_nonmissing': 0.16666666666666666,
        'missing_count': 0,
        'missing_percent': 0.0,
        'partial_unexpected_counts': [{'value': 'A', 'count': 1}, {'value': 'E', 'count': 5}],
        'partial_unexpected_index_list': [0, 10, 11, 12, 13, 14],
        'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
    }
}
```

```python
my_df.expect_column_values_to_be_in_set(
    "my_var",
    ["B", "C", "D", "F", "G", "H"],
    result_format={'result_format': 'COMPLETE'}
)
{
    'success': False,
    'result': {
        'unexpected_index_list': [0, 10, 11, 12, 13, 14],
        'unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
    }
}
```

## Behavior for `BOOLEAN_ONLY`

When the `result_format` is `BOOLEAN_ONLY`, no `result` is returned. The result of evaluating the Expectation is  
exclusively returned via the value of the `success` parameter.

For example:

```python
my_df.expect_column_values_to_be_in_set(
    "possible_benefactors",
    ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers"]
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
{
    'success': False
}

my_df.expect_column_values_to_be_in_set(
    "possible_benefactors",
    ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers", "Mr. Magwitch"]
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
{
    'success': False
}
```

## Behavior for `BASIC`

A `result` is generated with a basic justification for why an expectation was met or not. The format is intended 
for quick, at-a-glance feedback. For example, it tends to work well in Jupyter Notebooks.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of  
unexpected values to justify the expectation result.

The basic `result` includes:

```python
{
    "success" : Boolean,
    "result" : {
        "partial_unexpected_list" : [A list of up to 20 values that violate the expectation]
        "unexpected_count" : The total count of unexpected values in the column
        "unexpected_percent" : The overall percent of unexpected values
        "unexpected_percent_nonmissing" : The percent of unexpected values, excluding missing values from the denominator
    }
}
```

**Note:** When unexpected values are duplicated, `unexpected_list` will contain multiple copies of the value.

```python
[1,2,2,3,3,3,None,None,None,None]

expect_column_values_to_be_unique

{
    "success" : Boolean,
    "result" : {
        "partial_unexpected_list" : [2,2,3,3,3]
        "unexpected_count" : 5,
        "unexpected_percent" : 0.5,
        "unexpected_percent_nonmissing" : 0.8333333
    }
}
```


`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a single 
`observed_value` to justify the expectation result.

The basic `result` includes:

```python
{
    "success" : Boolean,
    "result" : {
        "observed_value" : The aggregate statistic computed for the column
    }
}
```

For example:

```python
[1, 1, 2, 2]

expect_column_mean_to_be_between

{
    "success" : Boolean,
    "result" : {
        "observed_value" : 1.5
    }
}
```


## Behavior for `SUMMARY`

A `result` is generated with a summary justification for why an expectation was met or not. The format is intended  
for more detailed exploratory work and includes additional information beyond what is included by `BASIC`.
For example, it can support generating dashboard results of whether a set of expectations are being met.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of  
unexpected values to justify the expectation result.

The summary `result` includes:

```python
{
    'success': False,
    'result': {
        'element_count': The total number of values in the column
        'unexpected_count': The total count of unexpected values in the column (also in `BASIC`)
        'unexpected_percent': The overall percent of unexpected values (also in `BASIC`)
        'unexpected_percent_nonmissing': The percent of unexpected values, excluding missing values from the denominator (also in `BASIC`)
        "partial_unexpected_list" : [A list of up to 20 values that violate the expectation] (also in `BASIC`)
        'missing_count': The number of missing values in the column
        'missing_percent': The total percent of missing values in the column
        'partial_unexpected_counts': [{A list of objects with value and counts, showing the number of times each of the unexpected values occurs}]
        'partial_unexpected_index_list': [A list of up to 20 of the indices of the unexpected values in the column]
    }
}
```

For example:

```python
{
    'success': False,
    'result': {
        'element_count': 36,
        'unexpected_count': 6,
        'unexpected_percent': 0.16666666666666666,
        'unexpected_percent_nonmissing': 0.16666666666666666,
        'missing_count': 0,
        'missing_percent': 0.0,
        'partial_unexpected_counts': [{'value': 'A', 'count': 1}, {'value': 'E', 'count': 5}],
        'partial_unexpected_index_list': [0, 10, 11, 12, 13, 14],
        'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
    }
}
```

`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a `observed_value` 
to justify the expectation result. It also includes additional information regarding observed values and counts, 
depending on the specific expectation.


The summary `result` includes:

```python
{
    'success': False,
    'result': {
        'observed_value': The aggregate statistic computed for the column (also in `BASIC`)
        'element_count': The total number of values in the column
        'missing_count':  The number of missing values in the column
        'missing_percent': The total percent of missing values in the column
        'details': {<expectation-specific result justification fields>}
    }
}
```

For example:

```python
[1, 1, 2, 2, NaN]

expect_column_mean_to_be_between

{
    "success" : Boolean,
    "result" : {
        "observed_value" : 1.5,
        'element_count': 5,
        'missing_count': 1,
        'missing_percent': 0.2
    }
}
```

## Behavior for `COMPLETE`

A `result` is generated with all available justification for why an expectation was met or not. The format is  
intended for debugging pipelines or developing detailed regression tests.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of 
unexpected values to justify the expectation result.

The complete `result` includes:

```python
{
    'success': False,
    'result': {
        "unexpected_list" : [A list of all values that violate the expectation]
        'unexpected_index_list': [A list of the indices of the unexpected values in the column]
        'element_count': The total number of values in the column (also in `SUMMARY`)
        'unexpected_count': The total count of unexpected values in the column (also in `SUMMARY`)
        'unexpected_percent': The overall percent of unexpected values (also in `SUMMARY`)
        'unexpected_percent_nonmissing': The percent of unexpected values, excluding missing values from the denominator (also in `SUMMARY`)
        'missing_count': The number of missing values in the column  (also in `SUMMARY`)
        'missing_percent': The total percent of missing values in the column  (also in `SUMMARY`)
    }
}
```

For example:

```python
{
    'success': False,
    'result': {
        'element_count': 36,
        'unexpected_count': 6,
        'unexpected_percent': 0.16666666666666666,
        'unexpected_percent_nonmissing': 0.16666666666666666,
        'missing_count': 0,
        'missing_percent': 0.0,
        'unexpected_index_list': [0, 10, 11, 12, 13, 14],
        'unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
    }
}
```

`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a `observed_value` 
to justify the expectation result. It also includes additional information regarding observed values and counts,  
depending on the specific expectation.

The complete `result` includes:

```python
{
    'success': False,
    'result': {
        'observed_value': The aggregate statistic computed for the column (also in `SUMMARY`)
        'element_count': The total number of values in the column (also in `SUMMARY`)
        'missing_count':  The number of missing values in the column (also in `SUMMARY`)
        'missing_percent': The total percent of missing values in the column (also in `SUMMARY`)
        'details': {<expectation-specific result justification fields, which may be more detailed than in `SUMMARY`>}
    }
}
```

For example:

```python
[1, 1, 2, 2, NaN]

expect_column_mean_to_be_between

{
    "success" : Boolean,
    "result" : {
        "observed_value" : 1.5,
        'element_count': 5,
        'missing_count': 1,
        'missing_percent': 0.2
    }
}
```
