---
title: Standard arguments for Expectations
---


All Expectations return a JSON-serializable dictionary when evaluated, and share four standard (optional) arguments:
* [result_format](#result_format): Controls what information is returned from the evaluation of the Expectation.
* [include_config](#include_config): If true, then the Expectation Suite itself is returned as part of the result
  object.
* [catch_exceptions](#catch_exceptions): If true, execution will not fail if the Expectation encounters an error.
  Instead, it will return success = False and provide an informative error message.
* [meta](#meta): Allows user-supplied meta-data to be stored with an Expectation.

All `ColumnMapExpectations` also have the following argument:
* [mostly](#mostly): A special argument that allows for _fuzzy_ validation based on some percentage (available for all `column_map_expectations`)

## `result_format`
See [Result format](./result_format.md) for more information.

## `include_config`

All Expectations accept a boolean `include_config` parameter. If true, then the Expectation Suite itself is returned as
part of the result object

```python
expect_column_values_to_be_in_set(
    "my_var",
    ['B', 'C', 'D', 'F', 'G', 'H'],
    result_format="COMPLETE",
    include_config=True,
)
# This returns:
{
    'exception_index_list': [0, 10, 11, 12, 13, 14],
    'exception_list': ['A', 'E', 'E', 'E', 'E', 'E'],
    'expectation_type': 'expect_column_values_to_be_in_set',
    'expectation_kwargs': {
        'column': 'my_var',
        'result_format': 'COMPLETE',
        'value_set': ['B', 'C', 'D', 'F', 'G', 'H']
    },
    'success': False
}
```

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

my_df.expect_column_values_to_be_between(
    "my_column",
    min_value=0,
    max_value=7
)
# This returns:
{
    "success": False,
    ...
}

my_df.expect_column_values_to_be_between(
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
my_df.expect_column_values_to_be_between(
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

## Dataset defaults

This default behavior for `result_format`, `include_config`, `catch_exceptions` can be overridden at the Dataset level:

```python
my_dataset.set_default_expectation_argument("result_format", "SUMMARY")
```

In validation mode, they can be overridden using flags:

```bash
great_expectations validation csv my_dataset.csv my_expectations.json \ 
--result_format=BOOLEAN_ONLY --catch_exceptions=False --include_config=True
```
