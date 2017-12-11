.. _standard_arguments:

================================================================================
Standard arguments for expectations
================================================================================

All expectations share four standard (optional) arguments:

* `include_config`
* `catch_exceptions`
* `meta`
* `output_format`

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


`meta`
------------------------------------------------------------------------------

All Expectations accept an optional `meta` parameter. If `meta` is a valid JSON-serializable dictionary, it will be passed through to the `expectation_result` object without modification.

.. code-block:: bash

    my_df.expect_column_values_to_be_in_set(
        "my_column",
        ["a", "b", "c"],
        meta={
            "foo": "bar",
            "baz": [1,2,3,4]
        }
    )
    {
        "result": False,
        "meta": {
            "foo": "bar",
            "baz": [1,2,3,4]
        }
    }

`output_format`
------------------------------------------------------------------------------

See :ref:`output_format` for more detail.


DataSet defaults
------------------------------------------------------------------------------

This default behavior for `output_format`, `include_config`, `catch_exceptions` can be overridden at the DataSet level:

.. code-block:: bash

    my_dataset.set_default_expectation_argument("output_format", "SUMMARY")

In validation mode, they can be overridden using flags:

.. code-block:: bash

    great_expectations my_dataset.csv my_expectations.json --output_format=BOOLEAN_ONLY --catch_exceptions=False --include_config=True

