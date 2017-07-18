.. _output_format:

================================================================================
Expectation result objects
================================================================================

Great Expectations tries return results that are contextually rich and informative, follows four basic principles:

* Result objects should be intuitive and self-documenting
* Result objects should be as consistent across expectations as reasonably possibly
* Result objects should be as flat as reasonably possible
* Result objects should help track data lineage

It isn't possible to fully satisfy all these criteria all the time. Here's how Great Expectations handles the tradeoffs.

`output_format`
------------------------------------------------------------------------------

All Expectations accept an `output_format` parameter. Out of the box, `output_format` can take 3 values: `boolean`, `basic`, and `summary`. That said, the API allows you to define new formats that mix, match, extend this initial set.

.. code-block:: bash

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
        "exception_list": ["aaaaA", "aaaaA", "bbbbB"],
        "exception_index_list": [2, 6, 7]
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
        "exception_count": 3
    }


The out-of-the-box default is `output_format=basic`. This default behavior can be overridden at the DataSet level:

.. code-block:: bash

    my_dataset.set_default_expectation_output_format(gerof.summary)

In validation mode, it can be overridden using the `output_format` flag:

.. code-block:: bash

    great_expectations my_dataset.csv my_expectations.json --output_format=summary

Note: accepting a single parameter for `output_format` should make the library of formats relatively easy to extend in the future.


Behavior for `basic` result objects
------------------------------------------------------------------------------
...depends on the expectation. Great Expectations has native support for three types of Expectations: `column_map_expectation`, `column_aggregate_expectation`, and a base type `expectation`.

`column_map_expectations` apply a boolean test function to each element within a column. The basic format is:

.. code-block:: bash

    {
        "success" : Boolean,
        "exception_list" : [A list of values that violate the expectation]
        "exception_index_list" : [A list of the indexes of those values]
    }


Note: when exception values are duplicated, `exception_list` will contain multiple copies of the value.

.. code-block:: bash

    [1,2,2,3,3,3]

    expect_column_values_to_be_unique

    {
        "success" : Boolean,
        "exception_list" : [2,2,3,3,3]
        "exception_index_list" : [1,2,3,4,5]
    }


`column_aggregate_expectations` compute a single value for the column.

Format:

.. code-block:: bash

    {
        "success" : Boolean,
        "true_value" : Depends
    }
    

For example:

.. code-block:: bash

    expect_table_row_count_to_be_between

    {
        "success" : true,
        "true_value" : 7
    }


    expect_column_stdev_to_be_between
    {
        "success" : false
        "true_value" : 3
    }

    expect_column_most_common_value_to_be
    {
        "success" : ...
        "true_value" : ...
    }


Behavior for `summary` result objects
------------------------------------------------------------------------------

... Generate a summary of common exception values. For `column_map_expectations`, the standard format is:

.. code-block:: bash

    {
        "success" : false,
        "exception_count" : 3,
        "exception_counts": {
            "aaaaA" : 2,
            "bbbbB": 1
        },
        "exception_percent": 0.3
    }
    
For `column_aggregate_expectations`, `summary` output is the same as `basic` output.


`include_kwargs`
------------------------------------------------------------------------------

In addition, all Expectations accept a boolean `include_kwargs` parameter. If true, then the expectation config itself is returned as part of the result object

.. code-block:: bash

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format=gerof.summary,
        include_kwargs=True
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

`catch_exceptions`
------------------------------------------------------------------------------


<<<THIS SECTION IS CURRENTLY BROKEN. MOST THINKING NEEDED.>>>

All Expectations accept a boolean `catch_exceptions` parameter. If true, the expectation will not fail if it encounters an error. Instead, it will return False and (in `basic` and `summary` modes) an informative error message

.. code-block:: bash

    {
        "result": False,
        "raised_exception": True,
        "exception_traceback": "..."
    }

`catch_errors` is on by default in command-line validation mode, and off by default in exploration mode.


...

...

continue to execute and return a best-effort...

For `column_map_expectations`, each error is treated as an exception. Errors are treated as 

.. code-block:: bash


    # column_map_expectation in basic mode with catch_errors=True

    {
        "result": False,
        "exception_list": [-1,None,-5,None]
        "exception_index_list": [1,10,15,24],
        "error_index_list": [10, 24]
    }

    # column_map_expectation in summary mode with catch_errors=True

    {
        "result": False,
        "exception_list"
        "error_index_list": [1001, 2405],
        }
    }

    # column_aggregate_expectation with catch_errors=True

    {
        "result": False,
        "exception_list"
        "error_index_list": [1001, 2405],
        }
    }

