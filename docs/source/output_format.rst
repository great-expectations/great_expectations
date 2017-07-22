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

All Expectations accept an `output_format` parameter. Out of the box, `output_format` can take 3 values: `BOOLEAN_ONLY`, `BASIC`, and `SUMMARY`. That said, the API allows you to define new formats that mix, match, extend this initial set.

.. code-block:: bash

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format="BOOLEAN_ONLY"
    )
    False

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format="BASIC"
    )
    {
        "result": False,
        "exception_list": ["aaaaA", "aaaaA", "bbbbB"],
        "exception_index_list": [2, 6, 7]
    }

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format="SUMMARY"
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


The out-of-the-box default is `output_format=BASIC`.

Note: accepting a single parameter for `output_format` should make the library of formats relatively easy to extend in the future.


Behavior for `BASIC` result objects
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


Behavior for `SUMMARY` result objects
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


`include_config`
------------------------------------------------------------------------------

In addition, all Expectations accept a boolean `include_config` parameter. If true, then the expectation config itself is returned as part of the result object

.. code-block:: bash

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format=gerof.summary,
        include_config=True
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

All Expectations accept a boolean `catch_exceptions` parameter. If true, execution will not fail if the Expectation encounters an error. Instead, it will return False and (in `BASIC` and `SUMMARY` modes) an informative error message

.. code-block:: bash

    {
        "result": False,
        "raised_exception": True,
        "exception_traceback": "..."
    }

`catch_exceptions` is on by default in command-line validation mode, and off by default in exploration mode.


DataSet defaults
------------------------------------------------------------------------------

This default behavior for `output_format`, `include_config`, `catch_exceptions` can be overridden at the DataSet level:

.. code-block:: bash

    my_dataset.set_default_expectation_argument("output_format", "SUMMARY")

In validation mode, they can be overridden using flags:

.. code-block:: bash

    great_expectations my_dataset.csv my_expectations.json --output_format=BOOLEAN_ONLY --catch_exceptions=False --include_config=True

