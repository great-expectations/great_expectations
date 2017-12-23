.. _standard_arguments:

================================================================================
Standard arguments for expectations
================================================================================

All expectations share four standard (optional) arguments:

* `include_config`
* `catch_exceptions`
* `meta`
* `output_format`

.. _include_config:

`include_config`
------------------------------------------------------------------------------

All Expectations accept a boolean `include_config` parameter. If true, then the expectation config itself is returned as part of the result object

.. code-block:: bash

    >> expect_column_values_to_be_in_set(
        "my_var",
        ['B', 'C', 'D', 'F', 'G', 'H'],
        output_format="COMPLETE",
        include_config=True,
    )

    {
        'exception_index_list': [0, 10, 11, 12, 13, 14],
        'exception_list': ['A', 'E', 'E', 'E', 'E', 'E'],
        'expectation_type': 'expect_column_values_to_be_in_set',
        'expectation_kwargs': {
            'column': 'my_var',
            'output_format': 'COMPLETE',
            'value_set': ['B', 'C', 'D', 'F', 'G', 'H']
        },
        'success': False
    }

.. _catch_exceptions:

`catch_exceptions`
------------------------------------------------------------------------------

All Expectations accept a boolean `catch_exceptions` parameter. If true, execution will not fail if the Expectation encounters an error. Instead, it will return False and (in `BASIC` and `SUMMARY` modes) an informative error message

.. code-block:: bash

    {
        "result": False,
        "raised_exception": True,
        "exception_traceback": "..."
    }

`catch_exceptions` is on by default in command-line validation mode, and off by default in exploration mode.


.. _meta:

`meta`
------------------------------------------------------------------------------

All Expectations accept an optional `meta` parameter. If `meta` is a valid JSON-serializable dictionary, it will be passed through to the `expectation_result` object without modification.

.. code-block:: bash

    >> my_df.expect_column_values_to_be_in_set(
        "my_column",
        ["a", "b", "c"],
        meta={
            "foo": "bar",
            "baz": [1,2,3,4]
        }
    )
    {
        "success": False,
        "meta": {
            "foo": "bar",
            "baz": [1,2,3,4]
        }
    }

`output_format`
------------------------------------------------------------------------------

See :ref:`output_format` for more detail.

.. _mostly:

`mostly`
------------------------------------------------------------------------------

`mostly` is a special argument that is automatically available in all `column_map_expectations`. `mostly` must be a float between 0 and 1. Great Expectations evaluates it as a percentage, allowing some wiggle room when evaluating expectations: as long as `mostly` percent of rows evaluate to `True`, the expectation returns `"success": True`.

.. code-block:: bash

    [0,1,2,3,4,5,6,7,8,9]

    >> my_df.expect_column_values_to_be_between(
        "my_column",
        min_value=0,
        max_value=7
    )
    {
        "success": False,
        ...
    }

    >> my_df.expect_column_values_to_be_between(
        "my_column",
        min_value=0,
        max_value=7,
        mostly=0.7
    )
    {
        "success": True,
        ...
    }

Expectations with `mostly` return exception lists even if they succeed:

.. code-block:: bash

    >> my_df.expect_column_values_to_be_between(
        "my_column",
        min_value=0,
        max_value=7,
        mostly=0.7
    )
    {
      "success": true
      "summary_obj": {
        "exception_percent": 0.2, 
        "partial_exception_index_list": [
          8,
          9
        ], 
        "partial_exception_list": [
          8, 
          9
        ], 
        "exception_percent_nonmissing": 0.2, 
        "exception_count": 2
      }
    }

DataSet defaults
------------------------------------------------------------------------------

This default behavior for `output_format`, `include_config`, `catch_exceptions` can be overridden at the DataSet level:

.. code-block:: bash

    my_dataset.set_default_expectation_argument("output_format", "SUMMARY")

In validation mode, they can be overridden using flags:

.. code-block:: bash

    great_expectations my_dataset.csv my_expectations.json --output_format=BOOLEAN_ONLY --catch_exceptions=False --include_config=True

