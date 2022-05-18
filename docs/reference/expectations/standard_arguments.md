---
title: Standard arguments for Expectations
---


All Expectations return a JSON-serializable dictionary when evaluated, and share four standard (optional) arguments:

* [result_format](#result_format): Controls what information is returned from the evaluation of the Expectation.
* [catch_exceptions](#catch_exceptions): If true, execution will not fail if the Expectation encounters an error.
  Instead, it will return success = False and provide an informative error message.
* [meta](#meta): Allows user-supplied meta-data to be stored with an Expectation.

:::note
The parameter [include_config](#include_config) is also a standard argument, but it currently has no functionality.  It remains an option purely to maintain backward compatability with legacy code.
:::
  
All `ColumnMapExpectations` also have the following argument:

* [mostly](#mostly): A special argument that allows for _fuzzy_ validation based on some percentage (available for all `column_map_expectations`)

## `result_format`
See [Result format](./result_format.md) for more information.

## `include_config`

The original behaviour of `include_config` was to indicate if the Expectation Suite itself is returned as part of the result.  However, this is now the permanent behaviour regardless of what `include_config` is set to.  The `include_config` parameter is only still included to maintain backward compatability for existing scripts that define it. All Expectations in an Expectation Suite include a `expectation_config` key in their results.

For example, if you are [creating Expectations using the dynamic process](../../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md) and you executed something like the following statement:

```python
results = validator.expect_column_values_to_be_in_set(
  "passenger_count",
  [1, 2, 3, 4, 5,6,7,8],
  include_config=False,
  result_format="COMPLETE")
print(results["expectation_config"])
)
```

Despite `include_config` being set to `False`, the print statement will still output the `expectation_config` value seen below:

```
{
  "expectation_context": {
    "description": null
  },
  "expectation_type": "expect_column_values_to_be_in_set",
  "kwargs": {
    "include_config": false,
    "result_format": "COMPLETE",
    "column": "passenger_count",
    "value_set": [
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
    ],
    "batch_id": "3aa0a5a68a2bbe5abd3b08ea9739616c"
  },
  "meta": {}
}
```

For this reason, it is perfectly valid to omit the `include_config` parameter when creating Expectations.

## `catch_exceptions`

All Expectations accept a boolean `catch_exceptions` parameter. If this parameter is set to True, then Great
Expectations will intercept any exceptions so that execution will not fail if the Expectation encounters an error.
Instead, if Great Excpectations catches an exception while evaluating an Expectation, the Expectation result will (
in `BASIC` and `SUMMARY` modes) return the following informative error message:

```python
{
    "result": False,
    "catch_exceptions": True,
    "exception_traceback": "..."
}
```

`catch_exceptions` is on by default in command-line validation mode, and off by default in exploration mode.

## `meta`

All Expectations accept an optional `meta` parameter. If `meta` is a valid JSON-serializable dictionary, it will be \
passed through to the `expectation_result` object without modification. The `meta` parameter can be used to add \
helpful markdown annotations to Expectations (shown below). These Expectation "notes" are rendered within \
Expectation Suite pages in Data Docs.

```python
my_df.expect_column_values_to_be_in_set(
    "my_column",
    ["a", "b", "c"],
    meta={
      "notes": {
        "format": "markdown",
        "content": [
          "#### These are expectation notes \n - you can use markdown \n - or just strings"
        ]
      }
    }
)
# This returns:
{
    "success": False,
    "meta": {
      "notes": {
        "format": "markdown",
        "content": [
          "#### These are expectation notes \n - you can use markdown \n - or just strings"
        ]
      }
    }
}
```

## `mostly`

`mostly` is a special argument that is automatically available in all `column_map_expectations`. `mostly` must be a 
float between 0 and 1. Great Expectations evaluates it as a percentage, allowing some wiggle room when evaluating 
Expectations: as long as `mostly` percent of rows evaluate to `True`, the Expectation returns `"success": True`.

```python
[0,1,2,3,4,5,6,7,8,9]

validator.expect_column_values_to_be_between(
    "my_column",
    min_value=0,
    max_value=7
)
# This returns:
{
    "success": False,
    ...
}

validator.expect_column_values_to_be_between(
    "my_column",
    min_value=0,
    max_value=7,
    mostly=0.7
)
# This returns:
{
    "success": True,
    ...
}
```

Expectations with `mostly` return exception lists even if they succeed:

```python
validator.expect_column_values_to_be_between(
    "my_column",
    min_value=0,
    max_value=7,
    mostly=0.7
)
# This returns:
{
  "success": true
  "result": {
    "unexpected_percent": 0.2,
    "partial_unexpected_index_list": [
      8,
      9
    ],
    "partial_unexpected_list": [
      8,
      9
    ],
    "unexpected_percent_nonmissing": 0.2,
    "unexpected_count": 2
  }
}
```

## Checkpoints and `result_format`

While `result_format`, `include_config`, and `catch_expectations` are all standard arguments for Expectations, the `result_format` argument is also a valid parameter when included in calls to the `run(...)` command of a Checkpoint.

:::note Reminder:
For more detailed information on how to define `result_format` values, please see [our reference guide on `result_format`](./result_format.md).
:::