---
title: Result format
---


The `result_format` parameter may be either a string or a dictionary which specifies the fields to return in `result`.
  * The following string values are supported:
    * `"BOOLEAN_ONLY"`, `"BASIC"`, `"SUMMARY"`, or `"COMPLETE"`. The default is `"BASIC"`. The behavior of each setting is described in the [examples](#examples) below.
  * For dictionary usage, `result_format` may include the following keys:
    * `result_format`: Sets the fields to return in result.
    * `unexpected_index_column_names`: Defines columns that can be used to identify unexpected results, for example primary key (PK) column(s) or other columns with unique identifiers. Supports multiple column names as a list.
    * `return_unexpected_index_query`: When running validations, a query (or a set of indices) will be returned that will
      allow you to retrieve the full set of unexpected results including any columns identified in `unexpected_index_column_names`.  Setting this value to `False` will 
      suppress the output (default is `True`).
    * `partial_unexpected_count`: Sets the number of results to include in partial_unexpected_count, if applicable. If 
      set to 0, this will suppress the unexpected counts.
    * `include_unexpected_rows`: When running validations, this will return the entire row for each unexpected value in
      dictionary form. When using `include_unexpected_rows`, you must explicitly specify `result_format` as well, and
      `result_format` must be more verbose than `BOOLEAN_ONLY`. *WARNING: *
  
  :::warning
  `include_unexpected_rows` returns EVERY row for each unexpected value; for large tables, this could return an 
  unwieldy amount of data.
  :::

## Configure result format
`result_format` can be specified for either a single Expectation or an entire Checkpoint. When configured at the Expectation-level, 
the configuration will not be persisted, and you will receive a `UserWarning`. We therefore recommend that the Expectation-level
configuration be used for exploratory analysis, with the final configuration added at the Checkpoint-level.

### Expectation-level configuration
To apply `result_format` to an Expectation, pass it into the Expectation. We will first need to obtain a Validator object instance (e.g. by running the `$ great_expectations suite new` command).

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_set"
```

### Checkpoint-level configuration
To apply `result_format` to every Expectation in a Suite, define it in your Checkpoint configuration under the `runtime_configuration` key.

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_checkpoint_example"
```

The results will then be stored in the Validation Result after running the Checkpoint.
:::note
The `unexpected_index_list`, as represented by primary key (PK) columns, is 
rendered in DataDocs when `COMPLETE` is selected. 

The `unexpected_index_query`, which for `SQL` and `Spark` is a query that allows you to retrieve the full set of 
unexpected values from the dataset, is also rendered by default when `COMPLETE` is selected. For `Pandas`, this parameter 
returns the full set of unexpected indices, which can also be used to retrieve the full set of unexpected values. This is returned
whether or not the `unexpected_index_column_names` are defined. 

To suppress this output, the `return_unexpected_index_query` parameter can be set to `False`. 

Regardless of how Result Format is configured, `unexpected_list` is never rendered in Data Docs.
:::

## Result format values and fields



### All Expectations
| Fields within `result`                | BOOLEAN_ONLY                       |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|------------------------------------|----------------|----------------|-----------------
|    details (dictionary)               | Defined on a per-Expectation basis |
### Column map Expectations (e.g. `ColumnMapExpectation`, `ColumnPairMapExpectation`, `MulticolumnMapExpectation`)
| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    element_count                      |no              |yes             |yes             |yes             |
|    missing_count                      |no              |yes             |yes             |yes             |
|    missing_percent                    |no              |yes             |yes             |yes             |
|    unexpected_count                   |no              |yes             |yes             |yes             |
|    unexpected_percent                 |no              |yes             |yes             |yes             |
|    unexpected_percent_nonmissing      |no              |yes             |yes             |yes             |
|    partial_unexpected_list            |no              |yes             |yes             |yes             |
|    partial_unexpected_index_list      |no              |no              |yes             |yes             |
|    partial_unexpected_counts          |no              |no              |yes             |yes             |
|    unexpected_index_list              |no              |no              |no              |yes             |
|    unexpected_index_query             |no              |no              |no              |yes             |
|    unexpected_list                    |no              |no              |no              |yes             |
### Column aggregate Expectations
| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    observed_value                     |no              |yes             |yes             |yes             |

### Example use cases for different result_format values

| `result_format` Setting               | Example use case                                             |
----------------------------------------|---------------------------------------------------------------
|    BOOLEAN_ONLY                       | Automatic validation. No result is returned.                 |
|    BASIC                              | Exploratory analysis in a notebook.                          |
|    SUMMARY                            | Detailed exploratory work with follow-on investigation.      |
|    COMPLETE                           | Debugging pipelines or developing detailed regression tests. |

## Examples

The following examples will use the data defined in the following Pandas DataFrame:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/pandas_df_for_result_format"
```

### Behavior for `BOOLEAN_ONLY`

When the `result_format` is `BOOLEAN_ONLY`, no `result` is returned. The result of evaluating the Expectation is  
exclusively returned via the value of the `success` parameter.

For example:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_boolean_example"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_boolean_example_output"
```

### Behavior for `BASIC`

For `BASIC` format, a `result` is generated with a basic justification for why an Expectation was met or not. The format is intended 
for quick, at-a-glance feedback. For example, it tends to work well in Jupyter Notebooks.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`ColumnAggregateExpectation` Expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of  
unexpected values to justify the Expectation result.

The basic `result` includes:

```python
{
    "success" : Boolean,
    "result" : {
        "partial_unexpected_list" : [A list of up to 20 values that violate the Expectation]
        "unexpected_count" : The total count of unexpected values in the column
        "unexpected_percent" : The overall percent of unexpected values
        "unexpected_percent_nonmissing" : The percent of unexpected values, excluding missing values from the denominator
    }
}
```

**Note:** When unexpected values are duplicated, `unexpected_list` will contain multiple copies of the value.

For example:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_set"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_set_output"
```

`ColumnAggregateExpectation` computes a single aggregate value for the column, and so returns a single 
`observed_value` to justify the Expectation result.

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

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_agg"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_agg_output"
```

### Behavior for `SUMMARY`

A `result` is generated with a summary justification for why an Expectation was met or not. The format is intended  
for more detailed exploratory work and includes additional information beyond what is included by `BASIC`.
For example, it can support generating dashboard results of whether a set of Expectations are being met.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`ColumnAggregateExpectation` Expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of
unexpected values to justify the Expectation result.

The summary `result` includes:

```python
{
    'success': False,
    'result': {
        'element_count': The total number of values in the column
        'unexpected_count': The total count of unexpected values in the column (also in `BASIC`)
        'unexpected_percent': The overall percent of unexpected values (also in `BASIC`)
        'unexpected_percent_nonmissing': The percent of unexpected values, excluding missing values from the denominator (also in `BASIC`)
        "partial_unexpected_list" : [A list of up to 20 values that violate the Expectation] (also in `BASIC`)
        'missing_count': The number of missing values in the column
        'missing_percent': The total percent of missing values in the column
        'partial_unexpected_counts': [{A list of objects with value and counts, showing the number of times each of the unexpected values occurs}
        'partial_unexpected_index_list': [A list of up to 20 of the indices of the unexpected values in the column, as defined by the columns in `unexpected_index_column_names`]
    }
}
```

For example:
```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_set"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_set_output"
```

`ColumnAggregateExpectation` computes a single aggregate value for the column, and so returns a `observed_value` 
to justify the Expectation result. It also includes additional information regarding observed values and counts, 
depending on the specific Expectation.

The summary `result` includes:

```python
{
    'success': False,
    'result': {
        'observed_value': The aggregate statistic computed for the column (also in `BASIC`)
    }
}
```

For example:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_agg"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_agg_output"
```


### Behavior for `COMPLETE`
A `result` is generated with all available justification for why an Expectation was met or not. The format is  
intended for debugging pipelines or developing detailed regression tests.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`ColumnAggregateExpectation` Expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of 
unexpected values to justify the Expectation result.

The complete `result` includes:

```python
{
    'success': False,
    'result': {
        "unexpected_list" : [A list of all values that violate the Expectation]
        'unexpected_index_list': [A list of the indices of the unexpected values in the column, as defined by the columns in `unexpected_index_column_names`]
        'unexpected_index_query': [A query that can be used to retrieve all unexpected values (SQL and Spark), or the full list of unexpected indices (Pandas)]
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
```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_set"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_set_output"
```

`ColumnAggregateExpectation` computes a single aggregate value for the column, and so returns a `observed_value` 
to justify the Expectation result. It also includes additional information regarding observed values and counts,  
depending on the specific Expectation.

The complete `result` includes:

```python
{
    'success': False,
    'result': {
        'observed_value': The aggregate statistic computed for the column (also in `SUMMARY`)
        'details': {<Expectation-specific result justification fields, which may be more detailed than in `SUMMARY`>}
    }
}
```

For example:
```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_agg"
```

Will return the following output:

```python name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_agg_output"
```