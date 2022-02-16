.. _standard_arguments:

================================================================================
Standard arguments for expectations
================================================================================

All Expectations return a json-serializable dictionary when evaluated, and share four standard (optional) arguments:

 - :ref:`result_format`: controls what information is returned from the evaluation of the expectation expectation.
 - :ref:`include_config`: If true, then the expectation suite itself is returned as part of the result object.
 - :ref:`catch_exceptions`: If true, execution will not fail if the Expectation encounters an error. Instead, it will \
   return success = False and provide an informative error message.
 - :ref:`meta`: allows user-supplied meta-data to be stored with an expectation.


`result_format`
------------------------------------------------------------------------------

See :ref:`result_format` for more information.

.. _include_config:

`include_config`
------------------------------------------------------------------------------

All Expectations accept a boolean `include_config` parameter. If true, then the expectation suite itself is returned as part of the result object

.. code-block:: bash

    >> expect_column_values_to_be_in_set(
        "my_var",
        ['B', 'C', 'D', 'F', 'G', 'H'],
        result_format="COMPLETE",
        include_config=True,
    )

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

.. _catch_exceptions:

`catch_exceptions`
------------------------------------------------------------------------------

All Expectations accept a boolean `catch_exceptions` parameter. If this parameter is set to True, then Great Expectations will intercept any exceptions so that execution will not fail if the Expectation encounters an error. Instead, if Great Excpectations catches an exception while evaluating an Expectation, the Expectation result will (in `BASIC` and `SUMMARY` modes) return the following informative error message:

.. code-block:: bash

    {
        "result": False,
        "catch_exceptions": True,
        "exception_traceback": "..."
    }

`catch_exceptions` is on by default in command-line validation mode, and off by default in exploration mode.


.. _meta:

`meta`
------------------------------------------------------------------------------

All Expectations accept an optional `meta` parameter. If `meta` is a valid JSON-serializable dictionary, it will be \
passed through to the `expectation_result` object without modification. The `meta` parameter can be used to add \
helpful markdown annotations to Expectations (shown below). These Expectation "notes" are rendered within \
Expectation Suite pages in Data Docs.

.. code-block:: bash

    >> my_df.expect_column_values_to_be_in_set(
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


.. _mostly:

`mostly`
------------------------------------------------------------------------------

`mostly` is a special argument that is automatically available in all `column_map_expectations`. `mostly` must be a \
float between 0 and 1. Great Expectations evaluates it as a percentage, allowing some wiggle room when evaluating \
expectations: as long as `mostly` percent of rows evaluate to `True`, the expectation returns `"success": True`.

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


Dataset defaults
------------------------------------------------------------------------------

This default behavior for `result_format`, `include_config`, `catch_exceptions` can be overridden at the Dataset level:

.. code-block:: bash

    my_dataset.set_default_expectation_argument("result_format", "SUMMARY")

In validation mode, they can be overridden using flags:

.. code-block:: bash

    great_expectations validation csv my_dataset.csv my_expectations.json --result_format=BOOLEAN_ONLY --catch_exceptions=False --include_config=True


