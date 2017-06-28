.. _output_format:

================================================================================
Expectation result objects
================================================================================

Principles
------------------------------------------------------------------------------

* Result objects should be intuitive and self-documenting
* Result objects should be as consistent across expectations as reasonably possibly
* Result objects should be as flat as reasonably possible
* Result objects should help track data lineage

It isn't possible to fully satisfy all these criteria all the time. Here's how Great Expectations handles the tradeoffs.

output_format
------------------------------------------------------------------------------

All Expectations accept an `output_format` parameter that can take 3 values: `boolean`, `basic`, and `summary`.

```
>> import ge.result_output_formats as gerof
>> expect_column_values_to_match_regex(
    "my_column",
    "[A-Z][a-z]+",
    output_format=gerof.boolean
)
False

>> expect_column_values_to_match_regex(
    "my_column",
    "[A-Z][a-z]+",
    output_format=gerof.basic
)
{
    "result": False,
    "exception_list": ["aaaaA", "aaaaA", "bbbbB"]
}

>> expect_column_values_to_match_regex(
    "my_column",
    "[A-Z][a-z]+",
    output_format=gerof.summary
)
{
    "result": False,
    "exception_counts": {
        "aaaaA" : 2,
        "bbbbB": 1
    },
    "exception_percent": 0.3
}
```

The out-of-the-box default is `output_format=basic`. This default behavior can be overridden at the DataSet level:

`my_dataset.set_default_expectation_output_format(gerof.summary)`

In validation mode, it can be overridden using the `output_format` flag:

`great_expectations my_dataset.csv my_expectations.json --output_format=summary`

Note: accepting a single parameter for `output_format` should make the library of formats relatively easy to extend in the future.

include_lineage
------------------------------------------------------------------------------

In addition, all Expectations accept a boolean `include_lineage` parameter. If true, then the expectation config itself is returned as part of the result object

```
>> expect_column_values_to_match_regex(
    "my_column",
    "[A-Z][a-z]+",
    output_format=gerof.summary,
    include_lineage=True
)
{
    "result": False,
    "exception_counts": {
        "aaaaA" : 2,
        "bbbbB": 1
    },
    "exception_percent": 0.3,
    "expectation_type" : "expect_column_values_to_match_regex",
    "expectation_kwargs" : {
        "regex" : "[A-Z][a-z]+"]
    }
}
```

Behavior for `basic` result objects
------------------------------------------------------------------------------
...depends on the expectation.

For `elementwise_expectations`, the standard format is:

```
{
    "success" : Boolean,
    "exception_list" : [A list of exceptions]
}
```

Note: when exception values are duplicated, `exception_list` should contain multiple copies of the value.

For expectations based on comparisons with a single value (most aggregation expectations), the name of the second field should correspond exactly with the name of the expectation.

Format:

    ```
    expect_*_to_...

    {
        "success" : Boolean,
        "true_*" : 
    }
    ```

For example:

    ```
    expect_table_row_count_to_be_between

    {
        "success" : Boolean,
        "true_table_row_count" : 
    }


    expect_column_stdev_to_be_between
    {
        "success" : ...
        "true_column_stdev" : ...
    }
    ```


Behavior for `summary` result objects
------------------------------------------------------------------------------

...depends on the expectation *and the data to which it's being applied.*

For `elementwise_expectations` where at least 90% of exceptions fall into 8 categories or fewer, the standard format is:

```
{
    "success" : Boolean,
    "exception_counts": {
        "aaaaA" : 2,
        "bbbbB": 1
    },
    "exception_percent": 0.3
}
```

If the exceptions are more evenly distributed, the format is

```
{
    "success" : Boolean,
    "partial_exception_list": [ "x", "y", "z", "x", "y", "z" ],
    "exception_percent": 0.3
}
```

In this case, `partial_exception_list` will contain up to 20 values that failed to match the expectation.

For single-value expectations, the `summary` output format is the same as `basic` output format.

