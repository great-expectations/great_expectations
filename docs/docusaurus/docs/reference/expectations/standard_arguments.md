---
title: Standard arguments for Expectations
---


All Expectations return a JSON-serializable dictionary when evaluated, and share four standard (optional) arguments:

* [result_format](#result_format): Controls what information is returned from the evaluation of the Expectation.
* [catch_exceptions](#catch_exceptions): If true, execution will not fail if the Expectation encounters an error.
  Instead, it will return success = False and provide an informative error message.
* [meta](#meta): Allows user-supplied meta-data to be stored with an Expectation.

All `ColumnMapExpectations` and `MultiColumnMapExpectation` also have the following argument:

* [mostly](#mostly): A special argument that allows for _fuzzy_ validation based on some percentage 
(available for all `column_map_expectations` and `multicolumn_map_expectations`)

## `result_format`

See [Result format](./result_format.md) for more information.


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
validator.expect_column_values_to_be_in_set(
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

`mostly` is a special argument that is automatically available in all `column_map_expectations` and
`multicolumn_map_expectations`. `mostly` must be a float between 0 and 1. Great Expectations evaluates
it as a percentage, allowing some wiggle room when evaluating Expectations: as long as `mostly` percent
of rows evaluate to `True`, the Expectation returns `"success": True`.

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

While `result_format` and `catch_expectations` are both standard arguments for Expectations, the `result_format` argument is also a valid parameter when included in calls to the `run(...)` command of a Checkpoint.

:::note Reminder:
For more detailed information on how to define `result_format` values, please see [our reference guide on `result_format`](./result_format.md).
:::