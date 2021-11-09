---
title: Evaluation Parameters
---

You can use Evaluation Parameters to configure Expectations to use dynamic values, such as a value from a previous step in a pipeline or a date relative to today.

To use Evaluation Paramters, we specifically tell Great Expectations
to use parameters that need to be computed or provided at validation time. During interactive development, we can even provide a
temporary value that should be used during the initial evaluation of the Expectation.

```python
my_df.expect_table_row_count_to_equal(
    value={"$PARAMETER": "upstream_row_count", "$PARAMETER.upstream_row_count": 10},
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```

This will return `{'success': True}`.

More typically, when validating Expectations, you can provide Evaluation Parameters that are only available at runtime:

```python
my_df.validate(
    expectation_suite=my_dag_step_config, 
    evaluation_parameters={"upstream_row_count": upstream_row_count}
)
```


## Evaluation Parameter expressions

Evaluation parameters can include basic arithmetic and temporal expressions.  For example, we might want
to specify that a new table's row count should be between 90 - 110 % of an upstream table's row count (or a count from a
previous run). Evaluation parameters support basic arithmetic expressions to accomplish that goal:

```python
my_df.expect_table_row_count_to_be_between(
    min_value={"$PARAMETER": "trunc(upstream_row_count * 0.9)"},
    max_value={"$PARAMETER": "trunc(upstream_row_count * 1.1)"}, 
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```
This will return `{'success': True}`.

We can also use the temporal expressions "now" and "timedelta". This example states that we expect values for the "load_date" column to be within the last week.

```python
my_df.expect_column_values_to_be_greater_than(
    column="load_date",
    min_value={"$PARAMETER": "now() - timedelta(weeks=1)"}
)
```

Evaluation Parameters are not limited to simple values, for example you could include a list as a parameter value:

```python
my_df.expect_column_values_to_be_in_set(
    "my_column", 
    value_set={"$PARAMETER": "runtime_values"}
)
my_df.validate(
    evaluation_parameters={"runtime_values": [1, 2, 3]}
)
```

However, it is not possible to mix complex values with arithmetic expressions.
