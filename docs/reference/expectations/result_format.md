---
title: Result format
---


The `result_format` parameter may be either a string or a dictionary which specifies the fields to return in `result`.
  * For string usage, see `result_format` values.
  * For dictionary usage, `result_format` which may include the following keys:
    * `result_format`: Sets the fields to return in result.
    * `unexpected_index_column_names`: Defines the primary key (PK) columns used to represent unexpected results.
    * `return_unexpected_index_query`: Boolean flag that can be used to suppress PK output.
    * `partial_unexpected_count`: Sets the number of results to include in partial_unexpected_count, if applicable. If 
      set to 0, this will suppress the unexpected counts.
    * `include_unexpected_rows`: When running validations, this will return the entire row for each unexpected value in
      dictionary form. When using `include_unexpected_rows`, you must explicitly specify `result_format` as well, and
      `result_format` must be more verbose than `BOOLEAN_ONLY`. *WARNING: *
  
  :::warning
  `include_unexpected_rows` returns EVERY row for each unexpected value; for large tables, this could return an 
  unwieldy amount of data.
  :::

## Configure Result Format
Result Format can be applied to either a single Expectation or an entire Checkpoint. When configured at the Expectation-level, 
the configuration will not be persisted, and you will receive a `UserWarning`. We therefore recommend that the Expectation-level
configuration be used for exploratory analysis, with the final configuration added at the Checkpoint-level.

### Expectation Level Config
To apply `result_format` to an Expectation, pass it into the Expectation. We will first need to obtain a (Validator)[] object instance by running the `$ great_expectations suite new` command.

```python name="result_format_complete_example_set"
```
In order to see those values at the Suite level, configure `result_format` in your Checkpoint configuration.

### Checkpoint Level Config
To apply `result_format` to every Expectation in a Suite, define it in your Checkpoint configuration under the `runtime_configuration` key.

```python name="result_format_checkpoint_example"
```

The results will then be stored in the Validation Result after running the Checkpoint.
:::note
Regardless of where Result Format is configured, `unexpected_list` is never rendered in Data Docs. `unexpected_index_list` is rendered when `COMPLETE` is selected. 
:::

## result_format values

Great Expectations supports four values for `result_format`: `BOOLEAN_ONLY`, `BASIC`, `SUMMARY`, and `COMPLETE`. The 
out-of-the-box default is `BASIC`. Each successive value includes more detail and so can support different use 
cases for working with Great Expectations, including interactive exploratory work and automatic validation.

## Fields defined for all Expectations
| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    element_count                      |no              |yes             |yes             |yes             |
|    missing_count                      |no              |yes             |yes             |yes             |
|    missing_percent                    |no              |yes             |yes             |yes             |
|    details (dictionary)               |Defined on a per-expectation basis                                 |
### Fields defined for `column_map_expectation` type Expectations
| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    unexpected_count                   |no              |yes             |yes             |yes             |
|    unexpected_percent                 |no              |yes             |yes             |yes             |
|    unexpected_percent_nonmissing      |no              |yes             |yes             |yes             |
|    partial_unexpected_list            |no              |yes             |yes             |yes             |
|    partial_unexpected_index_list      |no              |no              |yes             |yes             |
|    partial_unexpected_counts          |no              |no              |yes             |yes             |
|    unexpected_index_list              |no              |no              |no              |yes             |
|    unexpected_index_query             |no              |no              |no              |yes             |
|    unexpected_list                    |no              |no              |no              |yes             |
### Fields defined for `column_aggregate_expectation` type Expectations
| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    observed_value                     |no              |yes             |yes             |yes             |
|    details (e.g. statistical details) |no              |no              |yes             |yes             |

### Example use cases for different result_format values

| `result_format` Setting               | Example use case                                             |
----------------------------------------|---------------------------------------------------------------
|    BOOLEAN_ONLY                       | Automatic validation. No result is returned.                 |
|    BASIC                              | Exploratory analysis in a notebook.                          |
|    SUMMARY                            | Detailed exploratory work with follow-on investigation.      |
|    COMPLETE                           | Debugging pipelines or developing detailed regression tests. |

## Examples

The following examples will use the data defined in the following Pandas DataFrame:

```python name="pandas_df_for_result_format"
```

## Behavior for `BOOLEAN_ONLY`

When the `result_format` is `BOOLEAN_ONLY`, no `result` is returned. The result of evaluating the Expectation is  
exclusively returned via the value of the `success` parameter.

For example:

```python name="result_format_boolean_example"
```

Will return the following output:
```python
>>> print(validation_result.success)
False

>>> print(validation_result.result)
{}
```

#### Behavior for `BASIC`

For `BASIC` format, a `result` is generated with a basic justification for why an expectation was met or not. The format is intended 
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
```python name="result_format_basic_example_set"
```
```python
>>> print(validation_result.success)
False

>>> print(validation_result.result)
{
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
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

```python name="result_format_basic_example_agg"
```
```python
>>> print(validation_result.success)
True

>>> print(validation_result.result)
{'observed_value': 3.6666666666666665}
```


#### Behavior for `SUMMARY`

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
        'partial_unexpected_counts': [{A list of objects with value and counts, showing the number of times each of the unexpected values occurs}
        'partial_unexpected_index_list': [A list of up to 20 of the indices of the unexpected values in the column]
    }
}
```

For example:
```python name="result_format_summary_example_set"
```
```python
>>> print(validation_result.success)
False

>>> print(validation_result.result)
{
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
    "partial_unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "partial_unexpected_counts": [
        {"value": "E", "count": 5},
        {"value": "A", "count": 1},
    ],
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


#### Behavior for `COMPLETE`
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
        'unexpected_index_query': [A query that can be used to retrieve all unexpected values (SQL and Spark), or the full list of unexpected indices (for Pandas)]
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

```python name="result_format_complete_example_set"
```
```python
>>> print(validation_result.success)
False

>>> print(validation_result.result)
{
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
    "partial_unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "partial_unexpected_counts": [
        {"value": "E", "count": 5},
        {"value": "A", "count": 1},
    ],
    "unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "unexpected_index_query": [0, 10, 11, 12, 13, 14],  # For Spark and SQL, this will be a query that can be used to retrieve all unexpected results
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

#### In a `Checkpoint` configuration 
```python name="result_format_checkpoint_example"
```